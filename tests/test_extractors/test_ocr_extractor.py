import pytest

from src.data_pipeline_agent.config import AppConfig
from src.data_pipeline_agent.extractors.ocr_extractor import OcrExtractor


def test_extractor_initialization():
    config = AppConfig()
    extractor = OcrExtractor(config)
    assert extractor is not None
    assert extractor.config.llm.model == config.llm.model


@pytest.mark.skip(reason="Requires OPENROUTER_API_KEY and calls external API")
def test_extract_real(valid_image_b64):
    config = AppConfig()
    extractor = OcrExtractor(config)
    result = extractor.extract(valid_image_b64)
    assert len(result) > 0
    assert "Extracted Text" in result
