from __future__ import annotations

from pathlib import Path

from ..config import AppConfig
from ..extractors.image_reader import ImageReader
from ..extractors.markdown_exporter import MarkdownExporter
from ..extractors.ocr_extractor import OcrExtractor
from ..utils.logger import get_logger

logger = get_logger(__name__)


class ImageAgent:
    def __init__(self, config: AppConfig):
        self.config = config
        self.reader = ImageReader()
        self.ocr = OcrExtractor(config)
        self.exporter = MarkdownExporter()

    def extract(self, image_path: str, output_path: str | None = None) -> Path:
        image_path = Path(image_path)

        if output_path is None:
            output_path = image_path.with_suffix(".md")

        if not self.reader.supported(image_path):
            raise ValueError(
                f"Unsupported image format: {image_path.suffix}. "
                f"Supported: {', '.join(sorted(self.reader.SUPPORTED_FORMATS))}"
            )

        meta, image_base64 = self.reader.read(str(image_path))

        mime_map = {
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".webp": "image/webp",
            ".tiff": "image/tiff",
            ".tif": "image/tiff",
            ".bmp": "image/bmp",
        }
        mime = mime_map.get(image_path.suffix.lower(), "image/jpeg")

        ocr_text = self.ocr.extract(image_base64, mime_type=mime)

        result = self.exporter.export(meta, ocr_text, str(output_path))
        logger.info("extraction_complete", image=str(image_path), output=str(result))
        return result
