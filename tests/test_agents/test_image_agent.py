from pathlib import Path

import pytest

from src.data_pipeline_agent.agents.image_agent import ImageAgent
from src.data_pipeline_agent.config import AppConfig


@pytest.fixture
def config():
    return AppConfig()


def test_agent_initialization(config):
    agent = ImageAgent(config)
    assert agent is not None


def test_extract_unsupported_format(config, tmp_path):
    path = tmp_path / "test.pdf"
    path.write_text("not an image")

    agent = ImageAgent(config)
    with pytest.raises(ValueError, match="Unsupported image format"):
        agent.extract(str(path))


def test_extract_missing_file(config):
    agent = ImageAgent(config)
    with pytest.raises(FileNotFoundError):
        agent.extract("/nonexistent/image.png")


def test_extract_png_no_api(config, tmp_path):
    from PIL import Image

    img_path = tmp_path / "test.png"
    Image.new("RGB", (50, 50), color="red").save(img_path)

    agent = ImageAgent(config)

    with pytest.raises(Exception):
        agent.extract(str(img_path))
