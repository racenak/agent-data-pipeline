import os
import json
import uuid

import boto3
import clickhouse_connect
import great_expectations as gx
import httpx
import pandas as pd
from prefect import flow, get_run_logger, task

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
SEAWEEDFS_ENDPOINT = os.environ.get("SEAWEEDFS_ENDPOINT", "http://localhost:8333")
SEAWEEDFS_ACCESS_KEY = os.environ.get("SEAWEEDFS_ACCESS_KEY", "")
SEAWEEDFS_SECRET_KEY = os.environ.get("SEAWEEDFS_SECRET_KEY", "")
SEAWEEDFS_BUCKET = os.environ.get("SEAWEEDFS_BUCKET", "raw")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")


# ---------------------------------------------------------------------------- #
#                                 1. EXTRACT                                   #
# ---------------------------------------------------------------------------- #
@task(retries=3, retry_delay_seconds=5)
def extract_users():
    """Fetches raw user data from a public API."""
    logger = get_run_logger()
    logger.info("Starting data extraction...")

    url = "https://jsonplaceholder.typicode.com/users"
    response = httpx.get(url)
    response.raise_for_status()

    raw_data = response.json()
    logger.info(f"Successfully extracted {len(raw_data)} user records.")
    return raw_data


# ---------------------------------------------------------------------------- #
#                             2. LOAD RAW TO SEAWEEDFS                         #
# ---------------------------------------------------------------------------- #
@task
def load_raw_to_seaweedfs(raw_data):
    """Persists the raw payload as an S3 object in SeaweedFS for lineage/audit."""
    logger = get_run_logger()
    logger.info("Persisting raw data to SeaweedFS...")

    client = boto3.client(
        "s3",
        endpoint_url=SEAWEEDFS_ENDPOINT,
        aws_access_key_id=SEAWEEDFS_ACCESS_KEY,
        aws_secret_access_key=SEAWEEDFS_SECRET_KEY,
    )

    run_id = uuid.uuid4().hex
    key = f"users/{run_id}.json"

    client.put_object(Bucket=SEAWEEDFS_BUCKET, Key=key, Body=json.dumps(raw_data))
    logger.info(f"Raw data written to s3://{SEAWEEDFS_BUCKET}/{key}")
    return key


# ---------------------------------------------------------------------------- #
#                         3. SCHEMA VALIDATION (GE)                            #
# ---------------------------------------------------------------------------- #
@task
def schema_validation(raw_data):
    """Validates the raw structure/types with a Great Expectations suite.

    Returns {"passed": bool, "report": dict}.
    """
    logger = get_run_logger()
    logger.info("Running schema validation...")

    df = pd.DataFrame(raw_data)

    context = gx.get_context()
    data_source = context.data_sources.add_pandas("raw_pandas")
    data_asset = data_source.add_dataframe_asset("raw_users")
    batch_def = data_asset.add_batch_definition_whole_dataframe("batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    suite = gx.ExpectationSuite("schema_suite")
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="id"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="name"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="username"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="email"))
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column="company"))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(column="id", type_="int64")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="email", regex=r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
        )
    )

    results = batch.validate(suite)
    passed = results["success"]
    logger.info(f"Schema validation passed={passed}")
    return {"passed": passed, "report": results}


# ---------------------------------------------------------------------------- #
#                                4. TRANSFORM                                  #
# ---------------------------------------------------------------------------- #
@task
def transform_users(raw_data):
    """Clean and filter the extracted data."""
    logger = get_run_logger()
    logger.info("Starting data transformation...")

    transformed_data = []
    for user in raw_data:
        cleaned_user = {
            "id": user["id"],
            "name": user["name"],
            "username": user["username"].lower(),
            "email": user["email"].lower(),
            "company": user["company"]["name"],
        }
        transformed_data.append(cleaned_user)
    logger.info(f"Transformed {len(transformed_data)} records.")
    return transformed_data


# ---------------------------------------------------------------------------- #
#                         5. LOAD STAGING (CLICKHOUSE)                         #
# ---------------------------------------------------------------------------- #
@task
def load_staging_clickhouse(cleaned_data):
    """Loads the transformed data into the ClickHouse staging table."""
    logger = get_run_logger()
    logger.info("Starting data load to ClickHouse staging via clickhouse-connect...")

    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=8123,
        username="clickhouse",
        password="clickhouse",
        database="pipeline",
    )

    client.command(
        """
        CREATE TABLE IF NOT EXISTS users_staging (
            id Int32,
            name String,
            username String,
            email String,
            company String
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY id
        """
    )

    columns = ["id", "name", "username", "email", "company"]
    data_to_insert = [
        [u["id"], u["name"], u["username"], u["email"], u["company"]]
        for u in cleaned_data
    ]

    client.insert(table="users_staging", data=data_to_insert, column_names=columns)
    client.close()
    logger.info("Data successfully loaded into ClickHouse staging!")


# ---------------------------------------------------------------------------- #
#                      6. DATA QUALITY VALIDATION (GE)                         #
# ---------------------------------------------------------------------------- #
@task
def data_quality_validation(cleaned_data):
    """Validates business/data-quality rules with a second Great Expectations suite.

    Returns {"passed": bool, "report": dict}.
    """
    logger = get_run_logger()
    logger.info("Running data quality validation...")

    df = pd.DataFrame(cleaned_data)

    context = gx.get_context()
    data_source = context.data_sources.add_pandas("clean_pandas")
    data_asset = data_source.add_dataframe_asset("clean_users")
    batch_def = data_asset.add_batch_definition_whole_dataframe("batch")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    suite = gx.ExpectationSuite("data_quality_suite")
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="email")
    )
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="id"))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="username", regex=r"^[a-z0-9._]+$"
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="email", regex=r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
        )
    )

    results = batch.validate(suite)
    passed = results["success"]
    logger.info(f"Data quality validation passed={passed}")
    return {"passed": passed, "report": results}


# ---------------------------------------------------------------------------- #
#                             7. MERGE TO MART                                 #
# ---------------------------------------------------------------------------- #
@task
def merge_to_mart():
    """Merges the staging table into the ClickHouse mart table (deduplicated)."""
    logger = get_run_logger()
    logger.info("Merging staging into mart...")

    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=8123,
        username="clickhouse",
        password="clickhouse",
        database="pipeline",
    )

    client.command(
        """
        CREATE TABLE IF NOT EXISTS users_mart (
            id Int32,
            name String,
            username String,
            email String,
            company String
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY id
        """
    )

    client.command(
        """
        INSERT INTO users_mart (id, name, username, email, company)
        SELECT id, name, username, email, company
        FROM users_staging
        """
    )
    client.close()
    logger.info("Data successfully merged into mart!")


# ---------------------------------------------------------------------------- #
#                                 8. SLACK                                     #
# ---------------------------------------------------------------------------- #
@task
def send_slack(message: str, severity: str = "INFO"):
    """Posts a message to Slack. No-op when SLACK_WEBHOOK_URL is unset."""
    logger = get_run_logger()
    color = {
        "CRITICAL": "#FF0000",
        "WARNING": "#FF9900",
        "INFO": "#36A64F",
    }.get(severity, "#36A64F")

    if not SLACK_WEBHOOK_URL:
        logger.info(f"Slack not configured — skipping alert [{severity}]: {message}")
        return

    payload = {
        "attachments": [
            {
                "color": color,
                "blocks": [
                    {
                        "type": "header",
                        "text": {"type": "plain_text", "text": f"ETL Pipeline: {severity}"},
                    },
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": message},
                    },
                ],
            }
        ]
    }
    try:
        httpx.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        logger.info("Slack alert sent.")
    except Exception as e:
        logger.warning(f"Slack alert failed: {e}")


# ---------------------------------------------------------------------------- #
#                                9. THE FLOW                                   #
# ---------------------------------------------------------------------------- #
@flow(name="User ETL Pipeline Blueprint")
def main_etl_flow():
    """Agent-free data pipeline: extract -> raw store -> validate -> transform -> load -> DQ -> mart."""
    raw_users = extract_users()
    load_raw_to_seaweedfs(raw_users)

    schema = schema_validation(raw_users)
    if not schema["passed"]:
        send_slack(f"Schema validation FAILED:\n{schema['report']}", "CRITICAL")
        return

    clean_users = transform_users(raw_users)
    load_staging_clickhouse(clean_users)

    dq = data_quality_validation(clean_users)
    if not dq["passed"]:
        send_slack(f"Data quality validation FAILED:\n{dq['report']}", "CRITICAL")
        return

    merge_to_mart()
    send_slack(
        f"Pipeline succeeded: {len(clean_users)} records merged into mart.", "INFO"
    )


if __name__ == "__main__":
    main_etl_flow()
