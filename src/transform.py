from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def get_rejected_events(df: DataFrame) -> DataFrame:
    """Events with no customer key."""
    return df.filter(F.col("custkey").isNull())


def clean_web_events(df: DataFrame) -> DataFrame:
    """Remove bad rows, deduplicate, fix types."""
    return (df
        .filter(F.col("custkey").isNotNull())
        .dropDuplicates(["event_id"])
        .withColumn("amount", F.coalesce(F.col("amount").cast("decimal(10,2)"), F.lit(0)))
        .withColumn("event_time", F.to_timestamp(F.regexp_replace("event_time", "/", "-"))))