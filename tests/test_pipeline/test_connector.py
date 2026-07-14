import pandas as pd
import pytest

from src.data_pipeline_agent.config import AppConfig
from src.data_pipeline_agent.models.schemas import DataSource, FileFormat
from src.data_pipeline_agent.pipeline.connector import FileConnector


@pytest.fixture
def config(tmp_path):
    return AppConfig()


@pytest.fixture
def csv_file(tmp_path):
    path = tmp_path / "test.csv"
    pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}).to_csv(path, index=False)
    return DataSource(path=path, format=FileFormat.csv)


def test_read_csv(config, csv_file):
    connector = FileConnector(config)
    df = connector.read(csv_file)
    assert len(df) == 3
    assert list(df.columns) == ["a", "b"]


def test_read_missing_file(config):
    connector = FileConnector(config)
    ds = DataSource(path="/nonexistent.csv", format=FileFormat.csv)
    with pytest.raises(FileNotFoundError):
        connector.read(ds)


def test_unsupported_format(config, tmp_path):
    file_path = tmp_path / "test.csv"
    file_path.write_text("a,b\n1,2")
    connector = FileConnector(config)
    ds = DataSource(path=file_path, format=FileFormat.csv)
    ds.format = "txt"  # type: ignore[assignment]
    with pytest.raises(ValueError, match="Unsupported format"):
        connector.read(ds)
