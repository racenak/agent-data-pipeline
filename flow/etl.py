import httpx
from prefect import flow, task, get_run_logger
from clickhouse_driver import Client

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
        # Standardizing fields and filtering out sensitive/unnecessary data
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
#                                  3. LOAD                                     #
# ---------------------------------------------------------------------------- #
@task
def load_users(cleaned_data):
    """Loads the transformed data into ClickHouse using clickhouse-driver."""
    logger = get_run_logger()
    logger.info("Starting data load to ClickHouse...")

    # Open connection via context manager (safer than client.disconnect())
    # Adjust host, user, and password to match your setup
    with Client(host="localhost", port=9000, user="default", password="") as client:

        client.execute("CREATE DATABASE IF NOT EXISTS pipeline")
        client.execute("USE pipeline")

        client.execute("""
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

        # clickhouse-driver natively binds a list of dicts to the column names
        client.execute(
            "INSERT INTO users (id, name, username, email, company) VALUES",
            cleaned_data
        )

    logger.info("Data successfully loaded into ClickHouse!")

# ---------------------------------------------------------------------------- #
#                               4. THE FLOW                                    #
# ---------------------------------------------------------------------------- #
@flow(name="User ETL Pipeline Blueprint")
def main_etl_flow():
    """The main Prefect flow orchestration."""
    raw_users = extract_users()
    clean_users = transform_users(raw_users)
    load_users(clean_users)

if __name__ == "__main__":
    main_etl_flow()
