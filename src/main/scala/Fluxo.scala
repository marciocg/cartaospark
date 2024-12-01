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
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SaveMode

object Fluxo:

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
      col("545").as("MTI"),
      //entryMode,
      col("388").as("Resposta"),
    //   col("2501").as("Tokenizada"),
      col("067").as("Bandeira"),
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
      col("100"),
      col("999").as("Sexo")
    )
    val mtis = Seq("0100", "0110", "0120", "0130", "0200", "0210")
    val limpo = df.select(selectColunas*)
      .withColumn("100", regexp_replace(col("100"), ",", "").cast(IntegerType))
    //   .withColumn("100", format_number(regexp_replace(col("100"), ",", "").cast(IntegerType), 2))
      .filter(col("100") > 0)
      .filter(col("Moeda") === 986)      
      .filter(col("Resposta") === "00")
      .filter(col("MTI") === "0100" || col("MTI") === "0110" || col("MTI") === "0120" || col("MTI") === "0130" || col("MTI") === "0200" || col("MTI") === "0210")
      .filter(col("Sexo").contains("M") || col("Sexo").contains("F"))
      .filter(col("Bandeira").contains("V") || col("Bandeira").contains("M") || col("Bandeira").contains("E"))
      //.filter(col("MTI").isin(mtis*))            // usando varargs não traz nenhum resultado? pq?
      .drop(col("Moeda"))
      .drop(col("Resposta"))
      .drop(col("MTI"))
      .withColumnRenamed("100", "Valor")
    

    // val limpo = df.select(selectColunas*).withColumn("100", bround(col("100"), 2))

    limpo.show()

    limpo
  end montaBase


  def exec(limpo: Dataset[Row]): Unit =
    
    val sexoBandeiraIndexer = new StringIndexer()
      .setInputCols(Array("Sexo", "Bandeira"))
      .setOutputCols(Array("SexoNum", "BandeiraNum"))

    val modelsexoenc = sexoBandeiraIndexer.fit(limpo)
    val sexoBandeiraEncoded = modelsexoenc.transform(limpo)

//criando colunas de categorias numéricas
    val onehotencoder = new OneHotEncoder()
      .setInputCols(Array("Logo", "MCC", "BandeiraNum", "SexoNum"))
      .setOutputCols(Array("catLogo", "catMCC", "catBandeira", "catSexo"))

    val modelenc = onehotencoder.fit(sexoBandeiraEncoded)
    val encoded = modelenc.transform(sexoBandeiraEncoded)

//  Assembling features into a single column
    val assembler = new VectorAssembler()
     .setInputCols(Array("catLogo", "catMCC", "catBandeira", "catSexo", "Valor"))    // se for usar sem o oneHotEncoder, entao fica só Logo e MCC
     .setOutputCol("features")

    val saida = assembler.transform(encoded)

    saida.show()


    saida.write.mode(SaveMode.Ignore).parquet("data/agosto_saida.parquet")

    val treinoTesteArray = saida.randomSplit(Array(0.67, 0.33), Random.nextLong)

    val (treino, teste) = treinoTesteArray match {
      case Array(a, b) => 
        (a.drop("Logo").drop("MCC").drop("Valor").drop("catLogo").drop("catMCC").drop("Sexo").drop("SexoNum").drop("catSexo").drop("Bandeira").drop("BandeiraNum").drop("catBandeira"), 
        b.drop("Logo").drop("MCC").drop("Valor").drop("catLogo").drop("catMCC").drop("Sexo").drop("SexoNum").drop("catSexo").drop("Bandeira").drop("BandeiraNum").drop("catBandeira"))
    }

    treino.printSchema
    teste.printSchema

    val qtdTreino = treino.count()
    val qtdTeste = teste.count()
    println(s"quantidade de rows count no treino = $qtdTreino")
    println(s"quantidade de rows count no teste = $qtdTeste")

    val avaliador = new ClusteringEvaluator()

    for k <- 2 to 6
    do
      val kmeans = new KMeans()
        .setK(k)
        .setSeed(Random.nextLong)
        //.setPredictionCol("Valor")
        .setFeaturesCol("features")

        val modelo = kmeans.fit(treino)

        // Make predictions
        val previsoes = modelo.transform(teste)
        // previsoes.select("features", "prediction")
        //   .filter(not(col("prediction") === 0)).show()
        previsoes.printSchema()
        // Evaluate clustering by computing Silhouette score
        val silhouette = avaliador.evaluate(previsoes)
        println(s"Silhouette for $k with squared euclidean distance = $silhouette")

        // val explain = avaliador.explainParams()
        // println(s"explicação dos params = $explain")



/*  val metricas = avaliador.getMetrics(previsoes).silhouette()
    println(s"Silhouette 2 = $metricas") 

    val feats = avaliador.getFeaturesCol
    val preds = avaliador.getPredictionCol
    println(s"colunas de features e predictions = $feats e $preds")
  // Shows the result.
     println("Cluster Centers: ")
    modelo.clusterCenters.foreach(println)
 */    
  end exec

end Fluxo
