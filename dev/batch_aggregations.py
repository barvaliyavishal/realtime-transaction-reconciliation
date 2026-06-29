import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

schema = "transaction_id STRING, card_id STRING, amount DOUBLE, currency STRING, merchant STRING, status STRING, event_time TIMESTAMP"

spark = SparkSession. \
        builder \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-aws-bundle:1.9.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue.warehouse", "s3://vishal-transaction-lake/warehouse") \
        .config("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .appName("kafka_receiver") \
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df =spark.read.format("iceberg").load("glue.db.transactions_bronze")
df.createOrReplaceTempView("transactions_bronze")
df = spark.sql("SELECT merchant, date(event_time) as event_date, count(1) as total_transactions, SUM(amount) as total_amount FROM transactions_bronze GROUP BY merchant, date(event_time)")
df.show()