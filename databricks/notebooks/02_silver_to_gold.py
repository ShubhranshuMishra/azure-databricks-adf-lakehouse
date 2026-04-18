from pyspark.sql import functions as F

SILVER_PATH = "dbfs:/mnt/silver/orders"
GOLD_PATH = "dbfs:/mnt/gold/daily_sales"
GOLD_CATEGORY_PATH = "dbfs:/mnt/gold/category_sales"


daily_sales_df = (
    spark.read.format("delta").load(SILVER_PATH)
    .groupBy("order_date", "country")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("active_customers"),
    )
    .orderBy("order_date", "country")
)

category_sales_df = (
    spark.read.format("delta").load(SILVER_PATH)
    .groupBy("order_date", "country", "category")
    .agg(
        F.sum("revenue").alias("category_revenue"),
        F.sum("quantity").alias("units_sold"),
    )
    .orderBy("order_date", "country", "category")
)

daily_sales_df.write.format("delta").mode("overwrite").partitionBy("order_date").save(GOLD_PATH)
category_sales_df.write.format("delta").mode("overwrite").partitionBy("order_date").save(GOLD_CATEGORY_PATH)
