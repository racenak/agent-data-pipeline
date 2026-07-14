from pathlib import Path

from deepagents import create_deep_agent

from ..config import AppConfig
from ..models.schemas import PipelineJob
from ..models.state import PipelineState, AgentContext
from ..utils.logger import get_logger
from .executor import ExecutorAgent
from .monitor import MonitorAgent
from .planner import PlannerAgent

logger = get_logger(__name__)


class OrchestratorAgent:
    def __init__(self, config: AppConfig):
        self.config = config
        self.planner = PlannerAgent(config)
        self.executor = ExecutorAgent(config)
        self.monitor = MonitorAgent(config)
        self._agent = None

    def _build_agent(self):
        if self._agent is None:
            self._agent = create_deep_agent(
                model=self.config.llm.model,
                system_prompt=(
                    "You are a data pipeline orchestrator. "
                    "You plan, execute, and monitor batch data pipelines. "
                    "You delegate to sub-agents for planning, execution, and monitoring."
                ),
                tools=[
                    self.planner.create_plan,
                    self.executor.run_pipeline,
                    self.monitor.check_status,
                ],
            )
        return self._agent

    async def run(self, job: PipelineJob) -> PipelineState:
        state = PipelineState(job=job)
        ctx = AgentContext(state=state, agent=self._build_agent())
        logger.info("orchestrator_started", job=job.name)

        plan = await self.planner.create_plan(job)
        state.steps_completed.append("planning")
        logger.info("plan_created", steps=len(plan.steps))

        result = await self.executor.run_pipeline(job)
        state.validation_results = result.validation_results
        state.error = result.error
        state.steps_completed.append("execution")

        status = await self.monitor.check_status(job.name)
        logger.info("orchestrator_completed", job=job.name, status=status)

        return state
