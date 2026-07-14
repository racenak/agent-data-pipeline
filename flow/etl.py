import httpx
import clickhouse_connect
from prefect import flow, task, get_run_logger

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
#                                   3. LOAD                                    #
# ---------------------------------------------------------------------------- #
@task
def load_users(cleaned_data):
    """Loads the transformed data into ClickHouse using clickhouse-connect."""
    logger = get_run_logger()
    logger.info("Starting data load to ClickHouse via clickhouse-connect...")

    # Khởi tạo client kết nối (Nên khởi tạo trong Task để tránh lỗi serialization của Prefect)
    client = clickhouse_connect.get_client(
        host='clickhouse',
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
#                                4. THE FLOW                                   #
# ---------------------------------------------------------------------------- #
@flow(name="User ETL Pipeline Blueprint")
def main_etl_flow():
    """The main Prefect flow orchestration."""
    raw_users = extract_users()
    clean_users = transform_users(raw_users)
    load_users(clean_users)

if __name__ == "__main__":
    main_etl_flow()
