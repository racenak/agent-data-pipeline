from dataclasses import dataclass, field

from deepagents import create_deep_agent
from pydantic import BaseModel

from .schemas import PipelineJob, ValidationResult


class PipelineState(BaseModel):
    job: PipelineJob
    current_step: str = ""
    steps_completed: list[str] = []
    steps_failed: list[str] = []
    validation_results: list[ValidationResult] = []
    error: str | None = None


@dataclass
class AgentContext:
    state: PipelineState
    agent: "CompiledStateGraph | None" = None
    intermediate_results: dict = field(default_factory=dict)
