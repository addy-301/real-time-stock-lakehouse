from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

spark=SparkSession.builder \
    .appName("Stock Bronze Layer") \
    .master("spark://spark:7077") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
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
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("timestamp", LongType(), True),
])

df=spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock-ticks") \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df=df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

enriched_df=parsed_df \
    .withColumn("symbol", upper(col("symbol"))) \
    .withColumn("price", coalesce(col("price"), col("close")).cast("double")) \
    .withColumn("volume", col("volume").cast("long")) \
    .withColumn("timestamp", to_timestamp(from_unixtime(col("timestamp")))) \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("processing_date", current_date()) \
    .select("symbol", "price", "volume", "timestamp", "ingestion_time", "processing_date") \
    .filter(col("symbol").isNotNull() & col("price").isNotNull() & col("timestamp").isNotNull())

bronze_query=enriched_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3a://bronze/checkpoints/stock_ticks_v2") \
    .option("path", "s3a://bronze/stock_ticks") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

print("Bronze layer streaming started...")

bronze_query.awaitTermination()