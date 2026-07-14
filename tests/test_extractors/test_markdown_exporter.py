from pathlib import Path

from src.data_pipeline_agent.extractors.image_reader import ImageMetadata
from src.data_pipeline_agent.extractors.markdown_exporter import MarkdownExporter


def test_export_basic(tmp_path):
    exporter = MarkdownExporter()
    meta = ImageMetadata(
        filename="photo.jpg",
        file_size_bytes=204800,
        format="JPEG",
        width=1920,
        height=1080,
        mode="RGB",
    )
    ocr_text = "## Extracted Text\n\nHello world"

    out = tmp_path / "output.md"
    result = exporter.export(meta, ocr_text, out)

    assert result == out
    assert out.exists()

    content = out.read_text()
    assert "photo.jpg" in content
    assert "1920 × 1080" in content
    assert "200.0 KB" in content
    assert "Hello world" in content
    assert "---" in content


def test_export_with_exif(tmp_path):
    exporter = MarkdownExporter()
    meta = ImageMetadata(
        filename="test.png",
        file_size_bytes=5000,
        format="PNG",
        width=100,
        height=200,
        mode="RGBA",
        exif={"Make": "Canon", "Model": "EOS R5"},
        gps={"GPSLatitude": "40.7128"},
    )

    out = tmp_path / "test.md"
    exporter.export(meta, "OCR content", out)

    content = out.read_text()
    assert "Make" in content
    assert "Canon" in content
    assert "GPSLatitude" in content
    assert "40.7128" in content


def test_export_creates_parent_dirs(tmp_path):
    exporter = MarkdownExporter()
    meta = ImageMetadata(
        filename="img.jpg",
        file_size_bytes=1000,
        format="JPEG",
        width=10,
        height=10,
        mode="RGB",
    )
    out = tmp_path / "sub/deep/output.md"
    result = exporter.export(meta, "text", out)
    assert result.exists()
