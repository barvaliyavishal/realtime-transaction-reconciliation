from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("check").getOrCreate()
print("Spark version:", spark.version)
spark.stop()