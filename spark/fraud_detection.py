from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("merchant_category", StringType()),
    StructField("amount", IntegerType()),
    StructField("location", StringType())
])

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp("timestamp"))

fraud = parsed.filter(
    (col("amount") > 5000)
)

fraud.writeStream \
    .format("parquet") \
    .option("path", "/data/fraud") \
    .option("checkpointLocation", "/data/fraud_checkpoint") \
    .start()

valid = parsed.filter(col("amount") <= 5000)

valid.writeStream \
    .format("parquet") \
    .option("path", "/data/valid") \
    .option("checkpointLocation", "/data/valid_checkpoint") \
    .start()

spark.streams.awaitAnyTermination()
