from deepagents import create_deep_agent

from ..config import AppConfig
from ..models.schemas import PipelineJob, PipelineStep, StepType
from ..utils.logger import get_logger

logger = get_logger(__name__)


class PlannerAgent:
    def __init__(self, config: AppConfig):
        self.config = config
        self._agent = None

    def _build_agent(self):
        if self._agent is None:
            self._agent = create_deep_agent(
                model=self.config.llm.model,
                system_prompt=(
                    "You are a data pipeline planner. "
                    "Given a source dataset and target sink, you design "
                    "the optimal sequence of extract, transform, and load steps. "
                    "Consider file formats, schema compatibility, and data quality."
                ),
            )
        return self._agent

    async def create_plan(self, job: PipelineJob) -> PipelineJob:
        logger.info("planning_pipeline", source=job.source.path, sink=job.sink.path)

        if not job.steps:
            job.steps = [
                PipelineStep(type=StepType.extract, name="extract_source"),
                PipelineStep(type=StepType.transform, name="transform_data"),
                PipelineStep(type=StepType.load, name="load_to_sink"),
            ]

        logger.info("plan_complete", step_count=len(job.steps))
        return job
