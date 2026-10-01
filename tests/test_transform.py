from decimal import Decimal
from src.transform import clean_web_events, get_rejected_events

SCHEMA = "event_id int, custkey int, event_type string, amount string, event_time string"


def test_duplicates_removed(spark):
    df = spark.createDataFrame([
        (1, 10, "purchase", "5.00", "2026-09-01 10:00:00"),
        (1, 10, "purchase", "5.00", "2026-09-01 10:00:00"),
    ], SCHEMA)
    assert clean_web_events(df).count() == 1


def test_null_custkey_goes_to_rejected(spark):
    df = spark.createDataFrame([
        (1, None, "purchase", "5.00", "2026-09-01 10:00:00"),
        (2, 10, "purchase", "5.00", "2026-09-01 10:00:00"),
    ], SCHEMA)
    assert clean_web_events(df).count() == 1
    assert get_rejected_events(df).count() == 1


def test_null_amount_becomes_zero(spark):
    df = spark.createDataFrame([
        (1, 10, "page_view", None, "2026-09-01 10:00:00"),
    ], SCHEMA)
    row = clean_web_events(df).collect()[0]
    assert row["amount"] == Decimal("0.00")


def test_negative_amount_is_kept(spark):
    df = spark.createDataFrame([
        (1, 10, "refund", "-20.00", "2026-09-01 10:00:00"),
    ], SCHEMA)
    row = clean_web_events(df).collect()[0]
    assert row["amount"] == Decimal("-20.00")


def test_both_date_formats_parse_the_same(spark):
    df = spark.createDataFrame([
        (1, 10, "purchase", "5.00", "2026-09-02 09:30:00"),
        (2, 10, "purchase", "5.00", "2026/09/02 09:30:00"),
    ], SCHEMA)
    clean = clean_web_events(df)
    assert clean.select("event_time").distinct().count() == 1