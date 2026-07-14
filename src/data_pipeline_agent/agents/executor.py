from deepagents import create_deep_agent

from ..config import AppConfig
from ..models.schemas import PipelineJob, ValidationResult
from ..models.state import PipelineState
from ..pipeline.connector import FileConnector
from ..pipeline.loader import FileLoader
from ..pipeline.transformer import DataTransformer
from ..utils.logger import get_logger

logger = get_logger(__name__)


class ExecutorAgent:
    def __init__(self, config: AppConfig):
        self.config = config
        self.connector = FileConnector(config)
        self.transformer = DataTransformer(config)
        self.loader = FileLoader(config)

    async def run_pipeline(self, job: PipelineJob) -> PipelineState:
        state = PipelineState(job=job)
        logger.info("execution_started", job=job.name)

        try:
            df = self.connector.read(job.source)
            logger.info("data_extracted", rows=len(df), cols=len(df.columns))

            df = self.transformer.transform(df, job.steps)
            logger.info("data_transformed", rows=len(df))

            self.loader.write(df, job.sink)
            logger.info("data_loaded", path=str(job.sink.path))

            state.validation_results.append(
                ValidationResult(
                    step_name="pipeline",
                    passed=True,
                    row_count=len(df),
                    column_count=len(df.columns),
                )
            )
        except Exception as e:
            logger.error("execution_failed", error=str(e))
            state.error = str(e)

        return state
