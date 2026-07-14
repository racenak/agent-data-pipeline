import pandas as pd


class DataTools:
    @staticmethod
    def preview(path: str, n: int = 5) -> list[dict]:
        ext = path.rsplit(".", 1)[-1].lower()
        readers = {"csv": pd.read_csv, "json": pd.read_json, "parquet": pd.read_parquet}
        reader = readers.get(ext)
        if reader is None:
            raise ValueError(f"Cannot preview: {ext}")
        return reader(path, nrows=n).to_dict(orient="records")

    @staticmethod
    def get_schema(path: str) -> list[dict]:
        ext = path.rsplit(".", 1)[-1].lower()
        readers = {"csv": pd.read_csv, "json": pd.read_json, "parquet": pd.read_parquet}
        reader = readers.get(ext)
        if reader is None:
            raise ValueError(f"Cannot read: {ext}")
        df = reader(path, nrows=1)
        return [{"name": c, "dtype": str(df[c].dtype)} for c in df.columns]

    @staticmethod
    def summary(path: str) -> dict:
        ext = path.rsplit(".", 1)[-1].lower()
        readers = {"csv": pd.read_csv, "json": pd.read_json, "parquet": pd.read_parquet}
        reader = readers.get(ext)
        if reader is None:
            raise ValueError(f"Cannot read: {ext}")
        df = reader(path)
        return {
            "rows": len(df),
            "columns": list(df.columns),
            "dtypes": {c: str(df[c].dtype) for c in df.columns},
            "null_counts": df.isnull().sum().to_dict(),
            "memory_bytes": df.memory_usage(deep=True).sum(),
        }
