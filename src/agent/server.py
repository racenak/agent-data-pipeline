from __future__ import annotations

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from src.agent.graph import graph

app = FastAPI(title="Pipeline Monitor Agent")


class RunRequest(BaseModel):
    query: str


class RunResponse(BaseModel):
    result: str


@app.post("/runs", response_model=RunResponse)
async def run(req: RunRequest) -> RunResponse:
    config = {"configurable": {"thread_id": "main"}}
    result = await graph.ainvoke(
        {"messages": [("user", req.query)]},
        config,
    )
    messages = result.get("messages", [])
    if messages:
        content = messages[-1].content if hasattr(messages[-1], "content") else str(messages[-1])
    else:
        content = "No response"
    return RunResponse(result=content)


@app.get("/health")
async def health():
    return {"status": "ok"}


def main() -> None:
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
