from src.data_pipeline_agent.tools.file_tools import FileTools


def test_get_file_info(tmp_path):
    f = tmp_path / "test.csv"
    f.write_text("a,b\n1,2\n")
    info = FileTools.get_file_info(str(f))
    assert info["name"] == "test.csv"
    assert info["suffix"] == ".csv"
    assert info["exists"] is True


def test_list_files(tmp_path):
    (tmp_path / "a.csv").write_text("x")
    (tmp_path / "b.csv").write_text("y")
    files = FileTools.list_files(str(tmp_path), "*.csv")
    assert len(files) == 2
