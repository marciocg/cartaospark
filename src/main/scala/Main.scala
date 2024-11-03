// package cartaospark

import org.apache.spark.sql.SparkSession

object Main:
  def main(args: Array[String]): Unit =
    val spark = SparkSession.builder()
      .appName{"cartao-spark"}
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/1000CC.csv")

    df.show()
    df.printSchema()

