package cartaospark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import scala.util.Random
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.OneHotEncoder

object Trabalho:

  def montaBase(spark: SparkSession): Dataset[Row] = 
    val df = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      // .option("dateformat", "MM/yyyy")
      .option("timestampFormat", value = "yyyy-MM-dd HH:mm:ss")
      .option("sep", value = "|")
      .parquet("data/agosto.parquet")

    //val entryMode = col("368").as("Entry Mode").cast(IntegerType)
    // val valor = df("100").as("Valor") //.cast(IntegerType)
    val selectColunas = List(
      col("068").as("Logo"),
      //   col("545").as("MTI"),
      //entryMode,
      col("388").as("Resposta"),
    //   col("2501").as("Tokenizada"),
    //   col("067").as("Bandeira"),
      //   col("1902").as("Tam BIN"),
      col("363").as("MCC"),
      //   col("372").as("Tam Adq"),
      //   col("374").as("Cod Adq"),
      col("457").as("Moeda"),
      //   col("401").as("C Accpt ID"),
      //col("366").as("Pais"),
      //   col("035B").as("BIN"),
    //   col("609").as("Ind Empresarial"),
    //   col("1626").as("MCI"),
    //  valor,
         col("100")
    //   col("999").as("Sexo")
    )
    val limpo = df.select(selectColunas*)
      .withColumn("100", regexp_replace(col("100"), ",", "").cast(IntegerType))
    //   .withColumn("100", format_number(regexp_replace(col("100"), ",", "").cast(IntegerType), 2))
      .filter(col("100") > 0)
      .filter(col("Moeda") === 986)      
      .filter(col("Resposta") === "00")           
      .withColumnRenamed("100", "Valor")
      .drop(col("Moeda"))
      .drop(col("Resposta"))
    

    // val limpo = df.select(selectColunas*).withColumn("100", bround(col("100"), 2))

    limpo
  end montaBase


  def exec(limpo: Dataset[Row]): Unit =

    
    limpo.printSchema()
    limpo.summary()
    limpo.show()
    

    val kmeans = new KMeans().setK(4).setSeed(Random.nextLong)

//  Assembling features into a single column
    val onehotencoder = new OneHotEncoder()
      .setInputCols(Array("Logo", "MCC"))
      .setOutputCols(Array("catLogo", "catMCC"))

    val modelenc = onehotencoder.fit(limpo)
    val encoded = modelenc.transform(limpo)

/*     val assembler = new VectorAssembler()
    //  .setInputCols(Array("Bandeira", "Moeda", "Logo", "Sexo", "MCC"))
     .setInputCols(Array("Logo", "MCC"))
     .setOutputCol("features")
 */
    val assembler = new VectorAssembler()
     .setInputCols(Array("catLogo", "catMCC"))    // se for usar sem o oneHotEncoder, entao fica só Logo e MCC
     .setOutputCol("features")


    // val saida = assembler.transform(limpo)
    val saida = assembler.transform(encoded)

    val modelo = kmeans.fit(saida)

  // Make predictions
    val previsoes = modelo.transform(saida)
    previsoes.show()

  // Evaluate clustering by computing Silhouette score
    val avaliador = new ClusteringEvaluator()

    val silhouette = avaliador.evaluate(previsoes)
    println(s"Silhouette with squared euclidean distance = $silhouette")

  // Shows the result.
    println("Cluster Centers: ")
    modelo.clusterCenters.foreach(println)
    
  end exec

end Trabalho
