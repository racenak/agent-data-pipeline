from pathlib import Path


class FileTools:
    @staticmethod
    def list_files(directory: str, pattern: str = "*") -> list[str]:
        return [str(p) for p in Path(directory).glob(pattern) if p.is_file()]

    @staticmethod
    def get_file_size(path: str) -> int:
        return Path(path).stat().st_size

    @staticmethod
    def get_file_info(path: str) -> dict:
        p = Path(path)
        return {
            "name": p.name,
            "suffix": p.suffix,
            "size_bytes": p.stat().st_size,
            "exists": p.exists(),
        }
