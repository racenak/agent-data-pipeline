from pathlib import Path

import pytest

from src.data_pipeline_agent.extractors.image_reader import ImageReader


@pytest.fixture
def reader():
    return ImageReader()


def test_supported_formats(reader):
    assert reader.supported("photo.jpg")
    assert reader.supported("photo.png")
    assert reader.supported("photo.webp")
    assert reader.supported("photo.tiff")
    assert not reader.supported("photo.gif")
    assert not reader.supported("photo.pdf")


def test_read_missing_file(reader):
    with pytest.raises(FileNotFoundError, match="Image not found"):
        reader.read("/nonexistent/image.png")


def test_read_unsupported_format(reader, tmp_path):
    path = tmp_path / "test.txt"
    path.write_text("not an image")
    with pytest.raises(ValueError, match="Unsupported image format"):
        reader.read(path)


def test_read_png(reader, tmp_path):
    from PIL import Image

    path = tmp_path / "test.png"
    img = Image.new("RGB", (100, 200), color="red")
    img.save(path)

    meta, b64 = reader.read(path)
    assert meta.filename == "test.png"
    assert meta.width == 100
    assert meta.height == 200
    assert meta.format == "PNG"
    assert meta.file_size_bytes > 0
    assert isinstance(b64, str)
    assert len(b64) > 0


def test_read_jpg_with_exif(reader, tmp_path):
    from PIL import Image

    path = tmp_path / "test.jpg"
    img = Image.new("RGB", (640, 480), color="blue")
    img.save(path, exif=b"Exif\x00\x00")

    meta, _ = reader.read(path)
    assert meta.width == 640
    assert meta.height == 480
    assert meta.format == "JPEG"
