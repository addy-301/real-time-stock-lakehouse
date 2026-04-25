from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark=SparkSession.builder \
    .appName("Bronze to Silver") \
    .master("spark://spark-master:7077") \
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

print("Spark Session created successfully...")

bronze_path = "s3a://bronze/stock_ticks"
bronze_path_obj = spark._jvm.org.apache.hadoop.fs.Path(bronze_path)
fs = bronze_path_obj.getFileSystem(spark._jsc.hadoopConfiguration())
if not fs.exists(bronze_path_obj):
    print("Bronze Delta path not found: s3a://bronze/stock_ticks")
    print("Run the bronze consumer first so data is written before silver transformation.")
    spark.stop()
    raise SystemExit(1)

bronze_df=spark.read.format("delta").load(bronze_path)
print(f"Loaded Bronze data with {bronze_df.count()} records")

silver_df=bronze_df \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("volume", col("volume").cast("long")) \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("ingestion_time", col("ingestion_time").cast("timestamp")) \
    .withColumn("processing_date", col("processing_date").cast("date")) \
    .withColumn("price_change_pct", 
                round((col("price") - lag("price").over(
                    Window.partitionBy("symbol").orderBy("timestamp"))) / col("price") * 100, 4)) \
    .withColumn("market_cap_category", 
                when(col("price") * col("volume") > 1000000000, "Large")
                .when(col("price") * col("volume") > 100000000, "Mid")
                .otherwise("Small")) \
    .dropDuplicates(["symbol", "timestamp"]) \
    .filter(col("price") > 0) \
    .filter(col("volume") >= 0) \
    .na.drop(subset=["symbol", "price", "timestamp"])
print(f"Loaded Silver data with {silver_df.count()} records")

silver_df=silver_df \
    .withColumn("data_quality_score", lit(1.0)) \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("transformation_logic", lit("Bronze to Silver")) \
    .withColumn("source_system", lit("Bronze")) \
    .withColumn("validation_status", lit("Passed")) \
    .withColumn("validation_timestamp", current_timestamp()) \
    .withColumn("validation_details", lit("All checks passed successfully")) \
    
print("Added data quality metadata columns to Silver DataFrame")

silver_df.write \
    .format("delta") \
    .mode("append") \
    .save("s3a://silver/stock_ticks")

print("Silver data written successfully to S3")

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_stock_ticks
    USING DELTA
    LOCATION 's3a://silver/stock_ticks'
""")
