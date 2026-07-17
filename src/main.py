import asyncio

from graphs.monitoring import build_graph


async def main():
    graph = await build_graph()

    while True:
        question = input("> ")

        if question.lower() == "exit":
            break

        result = await graph.ainvoke(
            {
                "raw_logs": "",
                "job_metadata": {},
                "processed_logs": "",
                "analysis_result": {},
                "retry_count": 0,
                "missing_info_reason": "",
                "messages": [],
            }
        )

        print()
        print(result["analysis_result"])
        print()


asyncio.run(main())
