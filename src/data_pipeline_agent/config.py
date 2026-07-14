from pathlib import Path

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineConfig(BaseModel):
    raw_dir: Path = Field(default=Path("./data/raw"))
    processed_dir: Path = Field(default=Path("./data/processed"))
    output_dir: Path = Field(default=Path("./data/output"))

    supported_input_formats: list[str] = ["csv", "json", "parquet", "jsonl"]
    supported_output_formats: list[str] = ["csv", "json", "parquet", "jsonl"]

    chunk_size: int = 100_000
    max_file_size_mb: int = 500


class LLMConfig(BaseModel):
    model: str = "openrouter:openai/gpt-4o"
    temperature: float = 0.1
    max_tokens: int = 4096


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", env_nested_delimiter="__")

    llm: LLMConfig = LLMConfig()
    pipeline: PipelineConfig = PipelineConfig()

    @classmethod
    def from_yaml(cls, path: Path) -> "AppConfig":
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls(**data)

    @classmethod
    def load(cls, env: str = "development") -> "AppConfig":
        import os

        config_path = Path(f"configs/{env}.yaml")
        if config_path.exists():
            config = cls.from_yaml(config_path)
        else:
            config = cls()

        env_model = os.getenv("LLM_MODEL")
        if env_model:
            config.llm.model = env_model
        return config
