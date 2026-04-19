from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

spark=SparkSession.builder \
    .appName("Stock Bronze Layer") \
    .master("spark://spark:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("Spark session created successfully...")

schema=StructType([
    StructField("symbols", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("timestamp", TimestampType(), True),
])

df=spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock-ticks") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df=df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

enriched_df=parsed_df \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("date", current_date()) 

bronze_query=enriched_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "s3a://bronze/checkpoints/stock_ticks") \
    .option("path", "s3a://bronze/tables/stock_ticks") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

print("Bronze layer streaming started...")

bronze_query.awaitTermination()