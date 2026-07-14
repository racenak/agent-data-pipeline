import pandas as pd

from ..config import AppConfig
from ..models.schemas import DataSink, FileFormat
from ..utils.logger import get_logger

logger = get_logger(__name__)


class FileLoader:
    def __init__(self, config: AppConfig):
        self.config = config

    def write(self, df: pd.DataFrame, sink: DataSink) -> None:
        path = self.config.pipeline.output_dir / sink.path if not sink.path.is_absolute() else sink.path
        path.parent.mkdir(parents=True, exist_ok=True)

        writer = self._writer_for(sink.format)
        if writer is None:
            raise ValueError(f"Unsupported output format: {sink.format}")

        kwargs: dict = {}
        if sink.format in (FileFormat.csv, FileFormat.json, FileFormat.jsonl):
            kwargs["encoding"] = sink.encoding
        if sink.format == FileFormat.csv and sink.delimiter:
            kwargs["sep"] = sink.delimiter

        logger.info("writing_file", path=str(path), fmt=sink.format, rows=len(df))
        writer(df, path, **kwargs)

    def _writer_for(self, fmt: FileFormat):
        writers = {
            FileFormat.csv: lambda df, p, **kw: df.to_csv(p, index=False, **kw),
            FileFormat.json: lambda df, p, **kw: df.to_json(p, orient="records", **kw),
            FileFormat.jsonl: lambda df, p, **kw: df.to_json(p, orient="records", lines=True, **kw),
            FileFormat.parquet: lambda df, p, **kw: df.to_parquet(p, index=False, **kw),
        }
        return writers.get(fmt)
