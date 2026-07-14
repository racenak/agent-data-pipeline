from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field


class StepType(str, Enum):
    extract = "extract"
    transform = "transform"
    load = "load"
    validate = "validate"


class FileFormat(str, Enum):
    csv = "csv"
    json = "json"
    jsonl = "jsonl"
    parquet = "parquet"


class DataSource(BaseModel):
    path: Path
    format: FileFormat
    delimiter: str | None = None
    encoding: str = "utf-8"
    schema_override: dict[str, str] | None = None


class DataSink(BaseModel):
    path: Path
    format: FileFormat
    delimiter: str | None = None
    encoding: str = "utf-8"
    if_exists: str = "replace"


class PipelineStep(BaseModel):
    type: StepType
    name: str
    config: dict = Field(default_factory=dict)


class ValidationResult(BaseModel):
    step_name: str
    passed: bool
    row_count: int
    column_count: int
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class PipelineJob(BaseModel):
    name: str
    source: DataSource
    sink: DataSink
    steps: list[PipelineStep] = Field(default_factory=list)
    description: str | None = None
