import pandas as pd


class ValidationTools:
    @staticmethod
    def check_missing(df: pd.DataFrame) -> dict:
        return {col: int(df[col].isnull().sum()) for col in df.columns}

    @staticmethod
    def check_duplicates(df: pd.DataFrame, subset: list[str] | None = None) -> int:
        return int(df.duplicated(subset=subset).sum())

    @staticmethod
    def check_unique_constraint(df: pd.DataFrame, column: str) -> bool:
        return df[column].is_unique

    @staticmethod
    def describe(df: pd.DataFrame) -> dict:
        desc = df.describe(include="all")
        return desc.to_dict()
