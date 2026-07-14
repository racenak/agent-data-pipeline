from __future__ import annotations

from datetime import datetime
from pathlib import Path

from .image_reader import ImageMetadata
from ..utils.helpers import format_bytes
from ..utils.logger import get_logger

logger = get_logger(__name__)


class MarkdownExporter:
    def export(
        self,
        meta: ImageMetadata,
        ocr_text: str,
        output_path: str | Path,
    ) -> Path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        content = self._render(meta, ocr_text)
        path.write_text(content, encoding="utf-8")

        logger.info("markdown_written", path=str(path), chars=len(content))
        return path

    def _render(self, meta: ImageMetadata, ocr_text: str) -> str:
        lines = [
            "---",
            f"source: {meta.filename}",
            f"extracted_at: {datetime.now().isoformat()}",
            "---",
            "",
            f"# Extraction: {meta.filename}",
            "",
            "## File Metadata",
            "",
            f"| Property | Value |",
            "|---|---|",
            f"| Filename | `{meta.filename}` |",
            f"| Format | {meta.format} |",
            f"| Dimensions | {meta.width} × {meta.height} px |",
            f"| Color Mode | {meta.mode} |",
            f"| File Size | {format_bytes(meta.file_size_bytes)} |",
        ]

        if meta.exif:
            lines.extend([
                "",
                "## EXIF Metadata",
                "",
                "| Tag | Value |",
                "|---|---|",
            ])
            for k, v in sorted(meta.exif.items()):
                lines.append(f"| {k} | {v} |")

        if meta.gps:
            lines.extend([
                "",
                "## GPS Coordinates",
                "",
                "| Tag | Value |",
                "|---|---|",
            ])
            for k, v in meta.gps.items():
                lines.append(f"| {k} | {v} |")

        lines.extend([
            "",
            "## Extracted Content",
            "",
            ocr_text,
        ])

        return "\n".join(lines) + "\n"
