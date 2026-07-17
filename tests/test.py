import os

from dotenv import load_dotenv

import httpx

load_dotenv()

url = os.environ.get("SLACK_WEBHOOK_URL")
if url:
    httpx.post(
        url,
        json={"text": "ETL Pipeline completed successfully!"},
        timeout=10,
    )
