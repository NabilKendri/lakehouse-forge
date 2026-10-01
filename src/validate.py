from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def check_schema(df: DataFrame, expected: dict) -> None:
    """Fail if a column is missing or has a different type (schema drift)."""
    actual = dict(df.dtypes)
    for col, dtype in expected.items():
        if col not in actual:
            raise ValueError(f"Missing column: {col}")
        if actual[col] != dtype:
            raise ValueError(f"Column {col} is {actual[col]}, expected {dtype}")


def check_no_nulls(df: DataFrame, columns: list) -> None:
    for c in columns:
        n = df.filter(F.col(c).isNull()).count()
        if n > 0:
            raise ValueError(f"{n} null value(s) in {c}")


def check_unique(df: DataFrame, key: str) -> None:
    dupes = df.groupBy(key).count().filter("count > 1").count()
    if dupes > 0:
        raise ValueError(f"{dupes} duplicated value(s) in {key}")


def check_allowed_values(df: DataFrame, column: str, allowed: list) -> None:
    bad = df.filter(~F.col(column).isin(allowed)).count()
    if bad > 0:
        raise ValueError(f"{bad} invalid value(s) in {column}")