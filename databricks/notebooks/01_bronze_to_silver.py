from pyspark.sql import functions as F

RAW_PATH = "dbfs:/mnt/landing/orders/orders.csv"
BRONZE_PATH = "dbfs:/mnt/bronze/orders"
SILVER_PATH = "dbfs:/mnt/silver/orders"


def load_to_bronze() -> None:
    bronze_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(RAW_PATH)
        .withColumn("ingested_at", F.current_timestamp())
    )
    bronze_df.write.format("delta").mode("overwrite").save(BRONZE_PATH)


def transform_to_silver() -> None:
    silver_df = (
        spark.read.format("delta").load(BRONZE_PATH)
        .dropDuplicates(["order_id"])
        .withColumn("order_date", F.to_date("order_timestamp"))
        .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") > 0)
        .select(
            "order_id",
            "customer_id",
            "product_id",
            "country",
            "order_date",
            "quantity",
            "unit_price",
            "revenue",
            "ingested_at",
        )
    )
    silver_df.write.format("delta").mode("overwrite").partitionBy("order_date").save(SILVER_PATH)


load_to_bronze()
transform_to_silver()
