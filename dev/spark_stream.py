import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

schema = "transaction_id STRING, card_id STRING, amount DOUBLE, currency STRING, merchant STRING, status STRING, event_time TIMESTAMP"

spark = SparkSession. \
        builder \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "file:///C:/Users/owner/iceberg-warehouse") \
        .appName("kafka_receiver") \
        .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS local.db")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load()

parsed_df = df.withColumn("parsed_value", from_json(col("value").cast("string"), schema)) \
    .select("parsed_value.*") \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "file:///C:/Users/owner/iceberg-checkpoint") \
    .toTable("local.db.transactions_bronze") \
    .awaitTermination()
    
