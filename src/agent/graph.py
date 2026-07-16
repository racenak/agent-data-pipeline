from langchain_openrouter import ChatOpenRouter
from langgraph.prebuilt import create_react_agent

from src.agent.tools import tools

model = ChatOpenRouter(model="deepseek/deepseek-v4-flash")

system_prompt = (
    "You are a pipeline monitoring assistant. "
    "Your job is to check the health of the data pipeline by running tools. "
    "Use check_prefect_failures to find failed flow runs. "
    "Use check_clickhouse to query pipeline data. "
    "Report findings clearly and concisely."
)

graph = create_react_agent(
    model=model,
    tools=tools,
    prompt=system_prompt,
)
