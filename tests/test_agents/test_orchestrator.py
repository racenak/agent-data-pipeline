import os

import pytest

from src.data_pipeline_agent.agents.orchestrator import OrchestratorAgent
from src.data_pipeline_agent.config import AppConfig
from src.data_pipeline_agent.models.schemas import DataSink, DataSource, FileFormat, PipelineJob


@pytest.fixture
def config():
    return AppConfig()


@pytest.fixture
def sample_job():
    return PipelineJob(
        name="test_job",
        source=DataSource(path="input.csv", format=FileFormat.csv),
        sink=DataSink(path="output.parquet", format=FileFormat.parquet),
    )


@pytest.mark.skipif(
    not os.getenv("OPENROUTER_API_KEY"),
    reason="Requires OPENROUTER_API_KEY",
)
@pytest.mark.asyncio
async def test_orchestrator_run(config, sample_job):
    agent = OrchestratorAgent(config)
    state = await agent.run(sample_job)
    assert state.job.name == "test_job"
