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

batch_df = spark.table("glue.db.transactions_batch_agg")
batch_df_final  = batch_df.selectExpr("cast(event_date as date) as date", "merchant", "total_transactions as total_transaction_count", "total_amount")

stream_df = spark.sql("SELECT cast(window.start as date) as date, merchant, sum(transaction_count) as total_transaction_count, sum(total_amount) as total_amount FROM glue.db.transactions_stream_agg GROUP BY cast(window.start as date), merchant")

batch_df_final.createOrReplaceTempView("batch_agg")
stream_df.createOrReplaceTempView("stream_agg") 

report = spark.sql("""
                SELECT 
                        b.date, 
                        b.merchant, 
                        b.total_transaction_count as batch_total_transaction_count, 
                        s.total_transaction_count as stream_total_transaction_count, 
                        b.total_amount as batch_total_amount, 
                        s.total_amount as stream_total_amount ,
                        (b.total_transaction_count - s.total_transaction_count) as transaction_count_difference,
                        (b.total_amount - s.total_amount) as total_amount_difference,
                        CASE 
                            WHEN (b.total_transaction_count - s.total_transaction_count) = 0 AND (b.total_amount - s.total_amount) = 0 THEN 'MATCH'
                            ELSE 'MISMATCH'
                        END as reconciliation_status
                FROM batch_agg b 
                FULL OUTER JOIN stream_agg s 
                ON b.date = s.date AND b.merchant = s.merchant""")

report.writeTo("glue.db.reconciliation_report").createOrReplace()