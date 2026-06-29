import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-aws-bundle:1.9.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue.warehouse", "s3://vishal-transaction-lake/warehouse") \
    .config("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

spark.sql("SELECT * FROM glue.db.transactions_batch_agg").show()
spark.sql("SELECT cast(window.start as date) as start_date, merchant, sum(transaction_count) as total_transaction_count, sum(total_amount) as total_amount FROM glue.db.transactions_stream_agg GROUP BY cast(window.start as date), merchant").show()
