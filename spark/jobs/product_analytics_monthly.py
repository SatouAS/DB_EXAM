from datetime import date
from pyspark.sql import SparkSession, functions as F

# Тут подключение
PG_URL      = "jdbc:postgresql://postgres:5432/exam"
PG_PROPS    = {"user": "satou", "password": "satou", "driver": "org.postgresql.Driver"}

MONGO_USER = "satou"
MONGO_PASS = "satou"
MONGO_HOST = "mongo"
MONGO_PORT = 27017
MONGO_DB   = "mongo"
MONGO_COLL = "reviews"
MONGO_URI  = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"

# Здесь я поставил что отчетный день следующий после периода, если нужно реальный - datetime.utcnow().date()
processing_date = date(2025, 9, 1)
date_from = date(2025, 8, 1)
date_to   = date(2025, 8, 31)

# Блок подключения spark
spark = (
    SparkSession.builder.appName("product_analytics_monthly")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.6.0,"
        "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0"
    )
    .config("spark.mongodb.read.connection.uri", MONGO_URI)
    .getOrCreate()
)

# Запрос отчета за последние 30 дней (за 8 месяц) по разным колонкам
orders_df = (
    spark.read.format("jdbc")
    .option("url", PG_URL)
    .option("dbtable", "orders")
    .options(**PG_PROPS)
    .load()
    .filter(F.col("order_date").between(F.lit(date_from), F.lit(date_to)))
    .select("order_id", "order_date")
)

order_items_df = (
    spark.read.format("jdbc")
    .option("url", PG_URL)
    .option("dbtable", "order_items")
    .options(**PG_PROPS)
    .load()
    .select("order_id", "product_id", "quantity", "price")
)

sales_metrics = (
    order_items_df.join(orders_df, "order_id")
    .groupBy("product_id")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").cast("long").alias("total_quantity"),
        F.sum(F.col("quantity") * F.col("price")).cast("double").alias("total_revenue")
    )
)

# Блок mongo
reviews_raw = (
    spark.read.format("mongodb")
    .option("uri", MONGO_URI)
    .option("database", MONGO_DB)
    .option("collection", MONGO_COLL)
    .load()
    .select("product_id", "rating", "created_at")
    .withColumn("created_at", F.to_timestamp("created_at"))
    .filter(F.col("created_at").between(F.lit(date_from), F.lit(date_to)))
)

reviews_metrics = (
    reviews_raw
    .groupBy("product_id")
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.count("*").alias("total_reviews"),
        F.sum(F.when(F.col("rating") >= 4, 1).otherwise(0)).alias("positive_reviews"),
        F.sum(F.when(F.col("rating") <= 2, 1).otherwise(0)).alias("negative_reviews")
    )
)

# Блок объединения
result_df = (
    sales_metrics.join(reviews_metrics, "product_id", how="full_outer")
    .fillna(0, subset=["order_count", "total_quantity", "total_revenue",
                       "avg_rating", "positive_reviews",
                       "negative_reviews", "total_reviews"])
    .withColumn("processing_date", F.lit(processing_date))
    .select(
        "product_id", "total_quantity", "total_revenue", "order_count",
        "avg_rating", "positive_reviews", "negative_reviews",
        "total_reviews", "processing_date"
    )
)

# Запись в psql
(
    result_df.write
    .format("jdbc")
    .option("url", PG_URL)
    .option("dbtable", "product_analytics_monthly")
    .options(**PG_PROPS)
    .mode("append")
    .save()
)

spark.stop()