import pandas as pd
import pytest

from src.data_pipeline_agent.config import AppConfig
from src.data_pipeline_agent.models.schemas import DataSink, FileFormat
from src.data_pipeline_agent.pipeline.loader import FileLoader


@pytest.fixture
def config(tmp_path):
    return AppConfig()


@pytest.fixture
def df():
    return pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})


def test_write_csv(config, tmp_path, df):
    loader = FileLoader(config)
    sink = DataSink(path=tmp_path / "out.csv", format=FileFormat.csv)
    loader.write(df, sink)
    assert (tmp_path / "out.csv").exists()


def test_write_parquet(config, tmp_path, df):
    loader = FileLoader(config)
    sink = DataSink(path=tmp_path / "out.parquet", format=FileFormat.parquet)
    loader.write(df, sink)
    assert (tmp_path / "out.parquet").exists()


def test_unsupported_format(config, tmp_path, df):
    loader = FileLoader(config)
    sink = DataSink(path=tmp_path / "out.txt", format=FileFormat.csv)
    sink.format = "txt"  # type: ignore[assignment]
    with pytest.raises(ValueError, match="Unsupported output format"):
        loader.write(df, sink)
