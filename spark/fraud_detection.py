from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark with Kafka connector package
spark = SparkSession.builder \
    .appName("FraudDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("merchant_category", StringType()),
    StructField("amount", IntegerType()),
    StructField("location", StringType())
])

# Read from Kafka (using the internal 'kafka' hostname)
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp("timestamp"))

# Fraud Logic: Impossible Travel (Multiple countries in 10m) OR High Spend
windowed_fraud = parsed \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(window("event_time", "10 minutes"), col("user_id")) \
    .agg(
        approx_count_distinct("location").alias("country_count"),
        sum("amount").alias("total_amount"),
        first("merchant_category").alias("merchant_category") # Keep for reporting
    ) \
    .filter((col("country_count") > 1) | (col("total_amount") > 5000))

# Write Fraudulent Transactions
query_fraud = windowed_fraud.writeStream \
    .format("parquet") \
    .option("path", "/data/fraud") \
    .option("checkpointLocation", "/data/fraud_checkpoint") \
    .outputMode("append") \
    .start()

# Write Valid Transactions (Simple filter for the batch layer)
query_valid = parsed.filter(col("amount") <= 5000) \
    .writeStream \
    .format("parquet") \
    .option("path", "/data/valid") \
    .option("checkpointLocation", "/data/valid_checkpoint") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()