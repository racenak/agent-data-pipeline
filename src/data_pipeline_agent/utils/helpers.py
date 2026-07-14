from pathlib import Path


def format_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def parse_file_size(size_str: str) -> int:
    size_str = size_str.strip().upper()
    units = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
    for suffix, multiplier in units.items():
        if size_str.endswith(suffix):
            return int(float(size_str[: -len(suffix)]) * multiplier)
    return int(size_str)
