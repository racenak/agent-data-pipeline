import pandas as pd
import pytest

from src.data_pipeline_agent.config import AppConfig
from src.data_pipeline_agent.models.schemas import PipelineStep, StepType
from src.data_pipeline_agent.pipeline.transformer import DataTransformer


@pytest.fixture
def config():
    return AppConfig()


@pytest.fixture
def df():
    return pd.DataFrame({"a": [1, 2, None, 4], "b": ["x", "y", "z", None]})


def test_drop_columns(config, df):
    transformer = DataTransformer(config)
    step = PipelineStep(type=StepType.transform, name="drop_columns", config={"columns": ["b"]})
    result = transformer.transform(df, [step])
    assert list(result.columns) == ["a"]


def test_fill_missing_drop(config, df):
    transformer = DataTransformer(config)
    step = PipelineStep(type=StepType.transform, name="fill_missing", config={"strategy": "drop"})
    result = transformer.transform(df, [step])
    assert len(result) == 2


def test_fill_missing_fill(config, df):
    transformer = DataTransformer(config)
    step = PipelineStep(type=StepType.transform, name="fill_missing", config={"strategy": "fill", "fill_value": 0})
    result = transformer.transform(df, [step])
    assert result["a"].iloc[2] == 0


def test_rename_columns(config, df):
    transformer = DataTransformer(config)
    step = PipelineStep(type=StepType.transform, name="rename_columns", config={"mapping": {"a": "x", "b": "y"}})
    result = transformer.transform(df, [step])
    assert list(result.columns) == ["x", "y"]


def test_filter_rows(config, df):
    transformer = DataTransformer(config)
    step = PipelineStep(type=StepType.transform, name="filter_rows", config={"column": "a", "operator": ">", "value": 1})
    result = transformer.transform(df.dropna(), [step])
    assert len(result) == 1
