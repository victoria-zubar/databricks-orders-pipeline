from datetime import datetime
from src.transformations import valid_orders, rejected_orders, latest_per_order

SCHEMA = "order_id INT, order_total DOUBLE, order_ts TIMESTAMP, ingested_at TIMESTAMP, _rescued_data STRING"
T1, T2 = datetime(2026, 11, 1), datetime(2026, 11, 2)


def test_valid_orders_drops_null_total_and_rescued(spark):
    df = spark.createDataFrame([
        (1, 100.0, T1, T1, None),
        (2, None,  T1, T1, None),
        (3, 50.0,  T1, T1, '{"x":"abc"}'),
    ], SCHEMA)
    assert [r.order_id for r in valid_orders(df).collect()] == [1]


def test_valid_orders_keeps_negative_total(spark):
    df = spark.createDataFrame([(9, -20.0, T1, T1, None)], SCHEMA)
    assert valid_orders(df).count() == 1


def test_rejected_orders_lists_all_reasons(spark):
    df = spark.createDataFrame([(3, None, T1, T1, '{"order_total":"abc"}')], SCHEMA)
    reason = rejected_orders(df).first().reject_reason
    assert reason == "order_total_null, rescued_data_not_null"


def test_latest_per_order_keeps_newest_version(spark):
    df = spark.createDataFrame([
        (2, 75.0, T1, T1, None),
        (2, 80.0, T2, T2, None),
    ], SCHEMA)
    result = latest_per_order(df).collect()
    assert len(result) == 1
    assert result[0].order_total == 80.0