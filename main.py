##converte .csv para .parquet

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("cartao-spark").master("local[*]").getOrCreate()

sc = spark.sparkContext

df = spark.read.csv("data/maio.csv", header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss", sep="|")

df.write.mode("overwrite").parquet("data/maio.parquet")

df.show()

spark.stop()
