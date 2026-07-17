import os

import clickhouse_connect
import great_expectations as gx
import httpx
import pandas as pd
from prefect import flow, get_run_logger, task

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
AGENT_API_URL = os.environ.get("AGENT_API_URL", "http://127.0.0.1:2024")

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
#                                2. TRANSFORM                                  #
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
            "company": user["company"]["name"]
        }
        transformed_data.append(cleaned_user)
    logger.info(f"Transformed {len(transformed_data)} records.")
    return transformed_data


# ---------------------------------------------------------------------------- #
#                             2.5 VALIDATE                                     #
# ---------------------------------------------------------------------------- #
@task
def validate_users(data: list[dict]) -> list[dict]:
    """Validate transformed data with Great Expectations before loading."""
    logger = get_run_logger()
    logger.info("Validating data with Great Expectations...")

    df = pd.DataFrame(data)

    validator = gx.from_pandas(df)
    validator.expect_column_values_to_not_be_null("id")
    validator.expect_column_values_to_be_unique("id")
    validator.expect_column_values_to_not_be_null("name")
    validator.expect_column_values_to_not_be_null("email")
    validator.expect_column_values_to_match_regex("email", r"^[^@]+@[^@]+\.[^@]+$")
    validator.expect_column_values_to_not_be_null("username")
    validator.expect_column_values_to_be_in_type_list("id", ["int64", "int32"])

    results = validator.validate()

    stats = results.statistics()
    logger.info(
        f"GE validation: "
        f"{stats['successful_expectations']}/{stats['evaluated_expectations']} "
        f"expectations passed"
    )

    if not results["success"]:
        failures = [
            r["expectation_config"]["type"]
            for r in results["results"]
            if not r["success"]
        ]
        logger.error(f"Validation failed: {failures}")
        raise ValueError(f"Data validation failed: {failures}")

    logger.info("Data validation passed.")
    return data


# ---------------------------------------------------------------------------- #
#                                   3. LOAD                                    #
# ---------------------------------------------------------------------------- #
@task
def load_users(cleaned_data):
    """Loads the transformed data into ClickHouse using clickhouse-connect."""
    logger = get_run_logger()
    logger.info("Starting data load to ClickHouse via clickhouse-connect...")

    # Khởi tạo client kết nối (Nên khởi tạo trong Task để tránh lỗi serialization của Prefect)
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=8123,
        username='clickhouse',
        password='clickhouse',
        database='pipeline'
    )

    # 1. Tạo bảng bằng phương thức command()
    client.command("""
        CREATE TABLE IF NOT EXISTS users (
            id Int32,
            name String,
            username String,
            email String,
            company String
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY id
    """)

    # 2. Chuẩn bị dữ liệu dạng List of Lists (hoặc List of Tuples) để Insert tối ưu nhất
    columns = ['id', 'name', 'username', 'email', 'company']
    data_to_insert = [
        [user['id'], user['name'], user['username'], user['email'], user['company']]
        for user in cleaned_data
    ]

    # 3. Thực hiện insert siêu tốc bằng phương thức insert()
    client.insert(
        table='users',
        data=data_to_insert,
        column_names=columns
    )

    # Đóng kết nối sau khi hoàn thành
    client.close()
    logger.info("Data successfully loaded into ClickHouse!")

# ---------------------------------------------------------------------------- #
#                                4. AGENT MONITOR                              #
# ---------------------------------------------------------------------------- #
@task
def run_agent_monitor(logs: str, metadata: dict):
    """Invoke the monitoring agent via LangGraph API for analysis."""
    logger = get_run_logger()
    logger.info("Invoking monitoring agent...")
    try:
        resp = httpx.post(
            f"{AGENT_API_URL}/runs/wait",
            json={
                "assistant_id": "monitoring",
                "input": {
                    "raw_logs": logs,
                    "job_metadata": metadata,
                    "processed_logs": "",
                    "analysis_result": {},
                    "retry_count": 0,
                    "missing_info_reason": "",
                    "messages": [],
                },
            },
            timeout=120,
        )
        resp.raise_for_status()
        result = resp.json()
        analysis = result.get("output", {}).get("analysis_result", {})
        severity = analysis.get("severity", "UNKNOWN")
        logger.info(f"Agent analysis complete — severity={severity}")
        if severity == "CRITICAL":
            logger.error(f"CRITICAL: {analysis.get('error_summary', '')}")
    except Exception as e:
        logger.warning(f"Agent monitor skipped: {e}")


# ---------------------------------------------------------------------------- #
#                                5. THE FLOW                                   #
# ---------------------------------------------------------------------------- #
@flow(name="User ETL Pipeline Blueprint")
def main_etl_flow():
    """The main Prefect flow orchestration."""
    raw_users = extract_users()
    clean_users = transform_users(raw_users)
    validated_users = validate_users(clean_users)
    load_users(validated_users)

    metadata = {
        "pipeline_name": "User ETL Pipeline",
        "task_id": "etl-run",
        "records_extracted": len(raw_users),
        "records_validated": len(validated_users),
        "records_loaded": len(validated_users),
    }
    run_agent_monitor(logs="", metadata=metadata)

if __name__ == "__main__":
    main_etl_flow()
