import httpx
from prefect import flow, task


@task(retries=2, retry_delay_seconds=10)
def call_agent_api(query: str) -> str:
    resp = httpx.post(
        "http://agent-api:8000/runs",
        json={"query": query},
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json()["result"]


@flow(log_prints=True)
def run_pipeline_monitor(
    query: str = "Check for recent pipeline failures and report any issues.",
):
    result = call_agent_api(query)
    print(result)


if __name__ == "__main__":
    run_pipeline_monitor()
