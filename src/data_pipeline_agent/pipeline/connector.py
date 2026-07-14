import pandas as pd

from ..config import AppConfig
from ..models.schemas import DataSource, FileFormat
from ..utils.logger import get_logger

logger = get_logger(__name__)

_READERS = {
    FileFormat.csv: pd.read_csv,
    FileFormat.json: pd.read_json,
    FileFormat.jsonl: lambda p, **kw: pd.read_json(p, lines=True, **kw),
    FileFormat.parquet: pd.read_parquet,
}


class FileConnector:
    def __init__(self, config: AppConfig):
        self.config = config

    def read(self, source: DataSource) -> pd.DataFrame:
        path = self.config.pipeline.raw_dir / source.path if not source.path.is_absolute() else source.path

        if not path.exists():
            raise FileNotFoundError(f"Source not found: {path}")

        reader = _READERS.get(source.format)
        if reader is None:
            raise ValueError(f"Unsupported format: {source.format}")

        kwargs = {"encoding": source.encoding}
        if source.delimiter:
            kwargs["sep"] = source.delimiter
        if source.schema_override:
            kwargs["dtype"] = source.schema_override

        logger.info("reading_file", path=str(path), fmt=source.format)
        return reader(path, **kwargs)
