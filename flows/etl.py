import json
import uuid

import boto3
import clickhouse_connect
import great_expectations as gx
import httpx
import pandas as pd
from prefect import flow, get_run_logger, task
from prefect.variables import Variable


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

    bucket = Variable.get("seaweedfs_bucket", default="raw")
    client = boto3.client(
        "s3",
        endpoint_url=Variable.get("seaweedfs_endpoint", default="http://localhost:8333"),
        aws_access_key_id=Variable.get("seaweedfs_access_key", default=""),
        aws_secret_access_key=Variable.get("seaweedfs_secret_key", default=""),
    )

    run_id = uuid.uuid4().hex
    key = f"users/{run_id}.json"

    client.put_object(Bucket=bucket, Key=key, Body=json.dumps(raw_data))
    logger.info(f"Raw data written to s3://{bucket}/{key}")
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
    data_source = context.sources.add_or_update_pandas("raw_pandas")
    data_asset = data_source.add_dataframe_asset("raw_users")
    batch_request = data_asset.build_batch_request(dataframe=df)

    context.add_or_update_expectation_suite("schema_suite")
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="schema_suite"
    )

    validator.expect_column_to_exist("id")
    validator.expect_column_to_exist("name")
    validator.expect_column_to_exist("username")
    validator.expect_column_to_exist("email")
    validator.expect_column_to_exist("company")
    validator.expect_column_values_to_be_of_type("id", "int64")
    validator.expect_column_values_to_match_regex(
        "email", r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    )

    results = validator.validate()
    passed = results["success"]
    logger.info(f"Schema validation passed={passed}")
    return {"passed": passed, "report": results.to_json_dict()}


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
        host=Variable.get("clickhouse_host", default="localhost"),
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
    data_source = context.sources.add_or_update_pandas("clean_pandas")
    data_asset = data_source.add_dataframe_asset("clean_users")
    batch_request = data_asset.build_batch_request(dataframe=df)

    context.add_or_update_expectation_suite("data_quality_suite")
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="data_quality_suite"
    )

    validator.expect_column_values_to_not_be_null("email")
    validator.expect_column_values_to_be_unique("id")
    validator.expect_column_values_to_match_regex("username", r"^[a-z0-9._]+$")
    validator.expect_column_values_to_match_regex(
        "email", r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    )

    results = validator.validate()
    passed = results["success"]
    logger.info(f"Data quality validation passed={passed}")
    return {"passed": passed, "report": results.to_json_dict()}


# ---------------------------------------------------------------------------- #
#                             7. MERGE TO MART                                 #
# ---------------------------------------------------------------------------- #
@task
def merge_to_mart():
    """Merges the staging table into the ClickHouse mart table (deduplicated)."""
    logger = get_run_logger()
    logger.info("Merging staging into mart...")

    client = clickhouse_connect.get_client(
        host=Variable.get("clickhouse_host", default="localhost"),
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
    """Posts a message to Slack. No-op when the slack_webhook_url Variable is unset."""
    logger = get_run_logger()
    color = {
        "CRITICAL": "#FF0000",
        "WARNING": "#FF9900",
        "INFO": "#36A64F",
    }.get(severity, "#36A64F")

    webhook_url = Variable.get("slack_webhook_url", default="")
    if not webhook_url:
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
        httpx.post(webhook_url, json=payload, timeout=10)
        logger.info("Slack alert sent.")
    except Exception as e:
        logger.warning(f"Slack alert failed: {e}")


# ---------------------------------------------------------------------------- #
#                                9. THE FLOW                                   #
# ---------------------------------------------------------------------------- #
@flow(name="User ETL Pipeline Blueprint")
def main_etl_flow():
    """Agent-free data pipeline: extract -> raw store -> validate -> transform -> load -> DQ -> mart."""
    raw = extract_users.submit()

    seaweed = load_raw_to_seaweedfs.submit(raw)

    schema = schema_validation.submit(
        raw,
        wait_for=[seaweed]
    )

    clean = transform_users.submit(
        raw,
        wait_for=[schema]
    )

    stage = load_staging_clickhouse.submit(
        clean,
        wait_for=[clean]
    )

    dq = data_quality_validation.submit(
        clean,
        wait_for=[stage]
    )

    mart = merge_to_mart.submit(
        wait_for=[dq]
    )

    send_slack.submit(
        "Pipeline succeeded",
        "INFO",
        wait_for=[mart],
    )


if __name__ == "__main__":
    main_etl_flow()
