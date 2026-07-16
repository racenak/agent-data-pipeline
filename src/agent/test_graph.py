from langgraph.graph import StateGraph, MessagesState
from langchain_openrouter import ChatOpenRouter

model = ChatOpenRouter(
    model="deepseek/deepseek-v4-flash",
    temperature=0,
    openrouter_provider={
        "order": ["OpenRouter"]
    },
)

def chatbot(state: MessagesState):
    response = model.invoke(state["messages"])
    return {"messages": [response]}

workflow = StateGraph(MessagesState)

workflow.add_node("chatbot", chatbot)

workflow.set_entry_point("chatbot")
workflow.set_finish_point("chatbot")

graph = workflow.compile()
