from __future__ import annotations

import base64
from dataclasses import dataclass, field
from pathlib import Path

from ..utils.logger import get_logger

logger = get_logger(__name__)

try:
    from PIL import Image
    from PIL.ExifTags import TAGS as EXIF_TAGS
except ImportError:
    Image = None  # type: ignore[assignment]
    EXIF_TAGS = {}


@dataclass
class ImageMetadata:
    filename: str
    file_size_bytes: int
    format: str
    width: int
    height: int
    mode: str
    exif: dict = field(default_factory=dict)
    gps: dict | None = None


class ImageReader:
    SUPPORTED_FORMATS = {".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp", ".webp"}

    def read(self, path: str | Path) -> tuple[ImageMetadata, str]:
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"Image not found: {path}")

        if path.suffix.lower() not in self.SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported image format: {path.suffix}. "
                f"Supported: {', '.join(sorted(self.SUPPORTED_FORMATS))}"
            )

        if Image is None:
            raise ImportError("Pillow is required. Install with: pip install Pillow")

        img = Image.open(path)
        img.load()

        meta = self._extract_metadata(path, img)
        b64 = self._encode_image(path)

        logger.info(
            "image_read",
            path=str(path),
            fmt=meta.format,
            size=f"{meta.width}x{meta.height}",
        )

        return meta, b64

    def _extract_metadata(self, path: Path, img: Image.Image) -> ImageMetadata:
        exif_raw = img._getexif()
        exif = {}
        gps = {}

        if exif_raw:
            for tag_id, value in exif_raw.items():
                tag_name = EXIF_TAGS.get(tag_id, tag_id)
                if tag_name == "GPSInfo":
                    for gps_tag_id, gps_value in value.items():
                        gps_tag_name = EXIF_TAGS.get(gps_tag_id, gps_tag_id)
                        gps[gps_tag_name] = str(gps_value)
                else:
                    exif[tag_name] = str(value)

        return ImageMetadata(
            filename=path.name,
            file_size_bytes=path.stat().st_size,
            format=img.format or "",
            width=img.width,
            height=img.height,
            mode=img.mode,
            exif=exif,
            gps=gps or None,
        )

    def _encode_image(self, path: Path) -> str:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")

    def supported(self, path: str | Path) -> bool:
        return Path(path).suffix.lower() in self.SUPPORTED_FORMATS
