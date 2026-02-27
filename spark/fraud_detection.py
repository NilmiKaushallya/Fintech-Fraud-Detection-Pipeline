from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("FraudDetection") \
    .getOrCreate()

schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("merchant_category", StringType()),
    StructField("amount", IntegerType()),
    StructField("location", StringType())
])

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "transactions") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp("timestamp"))

# Window for 10 minutes
windowed = parsed \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window("event_time", "10 minutes"),
        col("user_id")
    ).agg(
    approx_count_distinct("location").alias("country_count"),
    sum("amount").alias("total_amount")
    )

fraud = windowed.filter(
    (col("country_count") > 1) |
    (col("total_amount") > 5000)
)

fraud.writeStream \
    .format("parquet") \
    .option("path", "/data/fraud") \
    .option("checkpointLocation", "/data/fraud_checkpoint") \
    .outputMode("append") \
    .start()

valid = parsed.filter(col("amount") <= 5000)

valid.writeStream \
    .format("parquet") \
    .option("path", "/data/valid") \
    .option("checkpointLocation", "/data/valid_checkpoint") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()
