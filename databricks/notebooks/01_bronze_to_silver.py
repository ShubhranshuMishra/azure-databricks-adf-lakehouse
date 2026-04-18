from pyspark.sql import functions as F

RAW_ORDERS_PATH = "dbfs:/mnt/landing/orders/orders.csv"
RAW_CUSTOMERS_PATH = "dbfs:/mnt/landing/customers/customers.csv"
RAW_PRODUCTS_PATH = "dbfs:/mnt/landing/products/products.csv"

BRONZE_ORDERS_PATH = "dbfs:/mnt/bronze/orders"
BRONZE_CUSTOMERS_PATH = "dbfs:/mnt/bronze/customers"
BRONZE_PRODUCTS_PATH = "dbfs:/mnt/bronze/products"

SILVER_PATH = "dbfs:/mnt/silver/orders"


def load_csv_to_bronze(source_path: str, target_path: str) -> None:
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(source_path)
        .withColumn("ingested_at", F.current_timestamp())
    )
    df.write.format("delta").mode("overwrite").save(target_path)


def load_to_bronze() -> None:
    load_csv_to_bronze(RAW_ORDERS_PATH, BRONZE_ORDERS_PATH)
    load_csv_to_bronze(RAW_CUSTOMERS_PATH, BRONZE_CUSTOMERS_PATH)
    load_csv_to_bronze(RAW_PRODUCTS_PATH, BRONZE_PRODUCTS_PATH)


def transform_to_silver() -> None:
    orders_df = spark.read.format("delta").load(BRONZE_ORDERS_PATH)
    customers_df = spark.read.format("delta").load(BRONZE_CUSTOMERS_PATH)
    products_df = spark.read.format("delta").load(BRONZE_PRODUCTS_PATH)

    silver_df = (
        orders_df.dropDuplicates(["order_id"])
        .withColumn("order_date", F.to_date("order_timestamp"))
        .withColumn("revenue", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") > 0)
        .join(customers_df.select("customer_id", "customer_segment", "customer_tier"), on="customer_id", how="left")
        .join(products_df.select("product_id", "product_name", "category"), on="product_id", how="left")
        .select(
            "order_id",
            "customer_id",
            "customer_segment",
            "customer_tier",
            "product_id",
            "product_name",
            "category",
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
