import asyncio
from pathlib import Path

from dotenv import load_dotenv

from .agents.image_agent import ImageAgent
from .agents.orchestrator import OrchestratorAgent
from .config import AppConfig
from .models.schemas import DataSink, DataSource, FileFormat, PipelineJob
from .utils.logger import get_logger, setup_logging

logger = get_logger(__name__)


def create_app(config: AppConfig | None = None) -> OrchestratorAgent:
    if config is None:
        config = AppConfig.load()
    return OrchestratorAgent(config)


async def run_job(
    source_path: str,
    output_path: str,
    source_format: str = "csv",
    output_format: str = "parquet",
    job_name: str = "default_pipeline",
) -> None:
    setup_logging()
    load_dotenv()

    config = AppConfig.load()
    orchestrator = create_app(config)

    job = PipelineJob(
        name=job_name,
        source=DataSource(path=Path(source_path), format=FileFormat(source_format)),
        sink=DataSink(path=Path(output_path), format=FileFormat(output_format)),
    )

    state = await orchestrator.run(job)

    if state.error:
        logger.error("pipeline_failed", error=state.error)
    else:
        logger.info("pipeline_succeeded", validations=len(state.validation_results))


def run_extract(
    image_path: str,
    output_path: str | None = None,
    model: str | None = None,
) -> None:
    setup_logging()
    load_dotenv()

    config = AppConfig.load()
    if model:
        config.llm.model = model

    agent = ImageAgent(config)
    result = agent.extract(image_path, output_path)
    print(f"Extracted to: {result}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="AI Data Pipeline Agent")
    sub = parser.add_subparsers(dest="command", required=True)

    pipe = sub.add_parser("pipeline", help="Run a data pipeline job")
    pipe.add_argument("source", help="Path to source file")
    pipe.add_argument("output", help="Path to output file")
    pipe.add_argument("--source-format", default="csv", choices=["csv", "json", "parquet", "jsonl"])
    pipe.add_argument("--output-format", default="parquet", choices=["csv", "json", "parquet", "jsonl"])
    pipe.add_argument("--job-name", default="default_pipeline")
    pipe.add_argument("--env", default="development")

    extract = sub.add_parser("extract", help="Extract text and metadata from an image to markdown")
    extract.add_argument("image", help="Path to input image file")
    extract.add_argument("output", nargs="?", help="Path to output markdown file (default: image path with .md)")
    extract.add_argument("--model", help="Vision model override (e.g. anthropic/claude-sonnet-4)")
    extract.add_argument("--env", default="development")

    args = parser.parse_args()

    if args.command == "pipeline":
        asyncio.run(run_job(args.source, args.output, args.source_format, args.output_format, args.job_name))
    elif args.command == "extract":
        run_extract(args.image, args.output, args.model)
