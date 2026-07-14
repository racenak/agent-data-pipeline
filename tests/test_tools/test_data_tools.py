import pandas as pd

from src.data_pipeline_agent.tools.data_tools import DataTools


def test_get_schema(tmp_path):
    path = tmp_path / "test.csv"
    pd.DataFrame({"a": [1], "b": ["x"]}).to_csv(path, index=False)
    schema = DataTools.get_schema(str(path))
    assert len(schema) == 2
    assert schema[0]["name"] == "a"


def test_summary(tmp_path):
    path = tmp_path / "test.csv"
    pd.DataFrame({"a": [1, 2, None], "b": ["x", "y", "z"]}).to_csv(path, index=False)
    summary = DataTools.summary(str(path))
    assert summary["rows"] == 3
    assert summary["null_counts"]["a"] == 1
