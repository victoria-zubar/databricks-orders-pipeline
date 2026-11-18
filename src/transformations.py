from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def valid_orders(df: DataFrame) -> DataFrame:
    """Рядки, що проходять перевірки якості: є сума і немає rescued data."""
    return df.filter(F.col("order_total").isNotNull() & F.col("_rescued_data").isNull())


def rejected_orders(df: DataFrame) -> DataFrame:
    """Відбраковані рядки з переліком усіх причин."""
    return (df
        .filter(F.col("order_total").isNull() | F.col("_rescued_data").isNotNull())
        .withColumn("reject_reason", F.concat_ws(", ",
            F.when(F.col("order_total").isNull(), F.lit("order_total_null")),
            F.when(F.col("_rescued_data").isNotNull(), F.lit("rescued_data_not_null")))))


def latest_per_order(df: DataFrame) -> DataFrame:
    """Найсвіжіша версія кожного замовлення."""
    w = Window.partitionBy("order_id").orderBy(F.desc("order_ts"), F.desc("ingested_at"))
    return df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")