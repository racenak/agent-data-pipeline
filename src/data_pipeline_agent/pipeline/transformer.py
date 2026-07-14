import pandas as pd

from ..config import AppConfig
from ..models.schemas import PipelineStep, StepType
from ..utils.logger import get_logger

logger = get_logger(__name__)


class DataTransformer:
    def __init__(self, config: AppConfig):
        self.config = config

    def transform(self, df: pd.DataFrame, steps: list[PipelineStep]) -> pd.DataFrame:
        for step in steps:
            if step.type != StepType.transform:
                continue
            logger.info("applying_transform", step=step.name)
            df = self._apply_step(df, step)
        return df

    def _apply_step(self, df: pd.DataFrame, step: PipelineStep) -> pd.DataFrame:
        config = step.config

        if step.name == "drop_columns":
            cols = config.get("columns", [])
            return df.drop(columns=[c for c in cols if c in df.columns])

        if step.name == "rename_columns":
            mapping = config.get("mapping", {})
            return df.rename(columns=mapping)

        if step.name == "filter_rows":
            col = config.get("column")
            op = config.get("operator", "==")
            val = config.get("value")
            if op == "==":
                return df[df[col] == val]
            if op == "!=":
                return df[df[col] != val]
            if op == ">":
                return df[df[col] > val]
            if op == "<":
                return df[df[col] < val]

        if step.name == "fill_missing":
            strategy = config.get("strategy", "drop")
            if strategy == "drop":
                return df.dropna()
            if strategy == "fill":
                fill_val = config.get("fill_value", 0)
                return df.fillna(fill_val)
            if strategy == "ffill":
                return df.ffill()

        if step.name == "cast_types":
            types = config.get("types", {})
            for col, dtype in types.items():
                if col in df.columns:
                    df[col] = df[col].astype(dtype)
            return df

        if step.name == "add_column":
            col = config.get("name")
            expr = config.get("expression")
            if col and expr:
                df[col] = expr
            return df

        logger.warning("unknown_transform", step=step.name)
        return df
