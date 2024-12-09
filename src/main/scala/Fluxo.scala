package cartaospark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import scala.util.Random
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SaveMode
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.Column

object Fluxo:

  def montaBase(spark: SparkSession, arquivo: String): Dataset[Row] = 
    val df = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      // .option("dateformat", "MM/yyyy")
      .option("timestampFormat", value = "yyyy-MM-dd HH:mm:ss")
      .option("sep", value = "|")
      .parquet(arquivo)

    //val entryMode = col("368").as("Entry Mode").cast(IntegerType)
    // val valor = df("100").as("Valor") //.cast(IntegerType)

    val dfcount = df.count()
    println(s"quantidade total de registros no arquivo = $dfcount")

    val selectColunas = List(
      col("068").as("Modalidade"),
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
      col("609").as("Ind Empresarial"),
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
      .filter(not(col("Ind Empresarial") === "S"))
      .filter(col("Bandeira").contains("V") || col("Bandeira").contains("M") || col("Bandeira").contains("E"))
      //.filter(col("MTI").isin(mtis*))            // usando varargs não traz nenhum resultado? pq?
      .drop(col("Moeda"))
      .drop(col("Resposta"))
      .drop(col("MTI"))
      .drop(col("Ind Empresarial"))
      .withColumnRenamed("100", "Valor")
    

    // val limpo = df.select(selectColunas*).withColumn("100", bround(col("100"), 2))

    val limpocount = limpo.count()
    println(s"quantidade de registros no arquivo limpo = $limpocount")

    limpo.show()

    limpo
  end montaBase


  def prepara(limpo: Dataset[Row], arquivo: String): Unit =
    
    val sexoBandeiraIndexer = new StringIndexer()
      .setInputCols(Array("Sexo", "Bandeira"))
      .setOutputCols(Array("SexoNum", "BandeiraNum"))

    val modelsexoenc = sexoBandeiraIndexer.fit(limpo)
    val sexoBandeiraEncoded = modelsexoenc.transform(limpo)

//criando colunas de categorias numéricas
    val onehotencoder = new OneHotEncoder()
      .setInputCols(Array("Modalidade", "MCC", "BandeiraNum", "SexoNum"))
      .setOutputCols(Array("catModalidade", "catMCC", "catBandeira", "catSexo"))

    val modelenc = onehotencoder.fit(sexoBandeiraEncoded)
    val encoded = modelenc.transform(sexoBandeiraEncoded)

//  Assembling features into a single column
    val assembler = new VectorAssembler()
     .setInputCols(Array("catModalidade", "catMCC", "catBandeira", "catSexo", "Valor"))    // se for usar sem o oneHotEncoder, entao fica só Modalidade e MCC
     .setOutputCol("features")

    val saida = assembler.transform(encoded)

    saida.show()

    saida.printSchema()

    //fazfpgrowth(saida, array("catModalidade", "catMCC", "catBandeira", "catSexo"))
    //fazfpgrowth(saida, array("features"))

    saida.write.mode(SaveMode.Overwrite).parquet(arquivo)

    println(s"Arquivo saida gravado para reexec ===================== $arquivo")
 /*    
    val treinoTesteArray = saida.randomSplit(Array(0.67, 0.33), Random.nextLong)

    val (treino, teste) = treinoTesteArray match {
      case Array(a, b) => 
        (a.drop("Modalidade").drop("MCC").drop("Valor").drop("catModalidade").drop("catMCC").drop("Sexo").drop("SexoNum").drop("catSexo").drop("Bandeira").drop("BandeiraNum").drop("catBandeira"), 
        b.drop("Modalidade").drop("MCC").drop("Valor").drop("catModalidade").drop("catMCC").drop("Sexo").drop("SexoNum").drop("catSexo").drop("Bandeira").drop("BandeiraNum").drop("catBandeira"))
    }

    treino.printSchema
    teste.printSchema

    val qtdTreino = treino.count()
    val qtdTeste = teste.count()
    println(s"quantidade de rows count no treino = $qtdTreino")
    println(s"quantidade de rows count no teste = $qtdTeste")

    val avaliador = new ClusteringEvaluator()

    for k <- 2 to 5
    do
      val kmeans = new KMeans()
        .setK(k)
        .setSeed(Random.nextLong)
        .setFeaturesCol("features")

        val modelo = kmeans.fit(treino)

        // Make predictions
        val previsoes = modelo.transform(teste)
        // previsoes.select("features", "prediction")
        //   .filter(not(col("prediction") === 0)).show()
        
        //previsoes.printSchema()

        println("Quantidade de registros por cluster: ")
        previsoes.groupBy(col("prediction")).count().as("qtd_por_cluster").show()

        println("Cluster Centers: ")
        modelo.clusterCenters.foreach(println)
        
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
 */     */
  end prepara

  def reexec (saida: Dataset[Row], ik: Int, fk: Int, arqmodelo: String, minsup: Double, minconfidence: Double): Unit =
    // saida.write.mode(SaveMode.Ignore).parquet("data/agosto_saida.parquet")

    // fazfpgrowth(saida, array("Modalidade", "MCC", "Bandeira", "Sexo"))
    // println("Faz FPGrowth do dataset:")
    // fazfpgrowth(saida, array("features"), minsup, minconfidence)

    println("Análise descritiva do dataset:")    
    saida.describe().show()

    val treinoTesteArray = saida.randomSplit(Array(0.67, 0.33), Random.nextLong)

    val (treino, teste) = treinoTesteArray match {
      case Array(a, b) => (a, b)
        // (a.drop("Modalidade").drop("MCC").drop("Valor").drop("catModalidade").drop("catMCC").drop("Sexo").drop("SexoNum").drop("catSexo").drop("Bandeira").drop("BandeiraNum").drop("catBandeira"), 
        // b.drop("Modalidade").drop("MCC").drop("Valor").drop("catModalidade").drop("catMCC").drop("Sexo").drop("SexoNum").drop("catSexo").drop("Bandeira").drop("BandeiraNum").drop("catBandeira"))
    }

    treino.printSchema
    teste.printSchema

    // salva os arquivos de treino e teste para executar análise
    treino.write.mode(SaveMode.Overwrite).parquet("data/agosto_saida_treino.parquet")
    teste.write.mode(SaveMode.Overwrite).parquet("data/agosto_saida_teste.parquet")

    val qtdTreino = treino.count()
    val qtdTeste = teste.count()
    println(s"quantidade de rows count no treino = $qtdTreino")
    println(s"quantidade de rows count no teste = $qtdTeste")

    val somaTreino = treino.agg(sum("Valor")).first()
    val somaTeste = teste.agg(sum("Valor")).first()
    println(s"somatório dos valores no treino = $somaTreino")
    println(s"somatório dos valores no teste = $somaTeste")

    println(s"Análise descritiva do conjunto de dados de teste")
    teste.describe().show()

    // println(s"Faz FPGrowth no dataset de teste:")   // nao funciona pq reclama q ja tem o `prediction` ?
    // fazfpgrowth(teste, array("features"), minsup, minconfidence)

    val avaliador = new ClusteringEvaluator()
    val qtshow = 5

    for k <- ik to fk
    do {
      val kmeans = new KMeans()
        .setK(k)
        .setSeed(Random.nextLong)
        .setFeaturesCol("features")

        val modelo = kmeans.fit(treino)

      // Salvar o modelo
        val nomemodelo: String = (s"$arqmodelo" +  k.toString() + " clusters")
        modelo.write.overwrite().save(nomemodelo)
        println(s"Modelo salvo: $nomemodelo")

        analisadadosesalva(modelo, teste, k, avaliador, qtshow, false)
    }
  end reexec

  def carregaBase(spark: SparkSession, arquivo: String): Dataset[Row] = 
    val df = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .parquet(arquivo)
    df
  
  end carregaBase

  def fazfpgrowth(df: Dataset[Row], arrCols: Column, minsup: Double, minconfidence: Double): Unit = 
    //https://spark.apache.org/docs/3.5.3/ml-frequent-pattern-mining.html#fp-growth

    // Construção do DataFrame com um array das colunas
    val arrayDF = df.withColumn("colunasArray", arrCols) //, "Valor"))
    //val arrayDF = df.withColumn("colunasArray", array("Modalidade", "MCC", "Bandeira", "Sexo")) //, "Valor"))

    val fpgrowth = new FPGrowth().setItemsCol("colunasArray").setMinSupport(minsup).setMinConfidence(minconfidence)
    val modelofp = fpgrowth.fit(arrayDF)
    
    // Display frequent itemsets.
    modelofp.freqItemsets.show()
    
    // Display generated association rules.
    modelofp.associationRules.show()
    
    // transform examines the input items against all the association rules and summarize the
    // consequents as prediction
    modelofp.transform(arrayDF).show()
    
  end fazfpgrowth

  def analisadadosesalva(modelo: KMeansModel, teste: Dataset[Row], k: Int, avaliador: ClusteringEvaluator, qtshow: Int, indGrava: Boolean): Unit = 
  
      // Make predictions
        val previsoes = modelo.transform(teste)
      // previsoes.select("features", "prediction")
      //   .filter(not(col("prediction") === 0)).show()
        // println("Schema do dataset de previsoes com " + k.toString() + " clusters: ")
        // previsoes.printSchema()

        // println("Cluster Centers: ")
        // modelo.clusterCenters.foreach(println)

        println("Quantidade de registros em cada cluster, para " + k.toString() + " clusters:")
        previsoes.groupBy(col("prediction")).count().as("qtd_por_cluster").show()

        //faz um describe de cada cluster na iteração
        for n <- 0 to k-1
        do { println(s"Análise descritiva no cluster $n para execução com $k clusters:")
            previsoes.select("Modalidade", "Bandeira", "MCC", "Sexo", "Valor", "prediction")
              .filter(col("prediction") === n).describe().show()
        }

        // println("Os 10 maiores valores de cada cluster:")
        for n <- 0 to k-1
        do { println(s"Os $qtshow maiores valores no cluster $n para $k clusters:")
            previsoes.select("Modalidade", "Bandeira", "MCC", "Sexo", "Valor")
              .filter(col("prediction") === n).sort(col("Valor").desc).show(qtshow)
        }

        for n <- 0 to k-1
        do { println(s"Os $qtshow menores valores no cluster $n para $k clusters:")
            previsoes.select("Modalidade", "Bandeira", "MCC", "Sexo", "Valor")
              .filter(col("prediction") === n).sort(col("Valor").asc).show(qtshow)
        }

        // println("Os 10 MCCs com maiores quantidades de transações em cada cluster:")
        for n <- 0 to k-1
        do { println(s"Os $qtshow MCCs com maiores quantidades de transações no cluster $n para $k clusters:")
           previsoes.filter(col("prediction") === n).groupBy(col("MCC"))
           .count() //.as("qtd_por_mcc")
           .sort(col("count").desc)
           .show(qtshow)
        }

        // println("As 10 Modalidades de cartão com maiores quantidades de transações em cada cluster:")
        for n <- 0 to k-1
        do { println(s"As $qtshow Modalidades de cartão com maiores quantidades de transações no cluster $n para $k clusters:")
           previsoes.filter(col("prediction") === n).groupBy(col("Modalidade"))
           .count()  //.as("qtd_por_mdld")
           .sort(col("count").desc)
           .show(qtshow) 
        }

        for n <- 0 to k-1
        do { println(s"As $qtshow Sexo com maiores quantidades de transações no cluster $n para $k clusters:")
           previsoes.filter(col("prediction") === n).groupBy(col("Sexo"))
           .count()  //.as("qtd_por_mdld")
           .sort(col("count").desc)
           .show(qtshow) 
        }

        for n <- 0 to k-1
        do { println(s"As $qtshow Bandeiras de cartão com maiores quantidades de transações no cluster $n para $k clusters:")
           previsoes.filter(col("prediction") === n).groupBy(col("Bandeira"))
           .count()  //.as("qtd_por_mdld")
           .sort(col("count").desc)
           .show(qtshow) 
        }

        // grava o resultado no arquivo parquet
        //previsoes.select("Modalidade", "Bandeira", "MCC", "Valor", "Sexo", "prediction")
        if (indGrava) {
          previsoes.write.mode(SaveMode.Overwrite)
           .option("header", "true")
           .parquet("data/agosto_saida_clusters_" + k.toString() + ".parquet")
        }
        // correlations:
        // val pcorr = previsoes.map(Tuple1.apply).toDF("features")
        // val Row(coeff1: Matrix) = Correlation.corr(previsoes, "features").head
        // println(s"Pearson correlation matrix:\n $coeff1")

        // Evaluate clustering by computing Silhouette score
         val silhouette = avaliador.evaluate(previsoes)
         
         println(s"O Silhouette calculado para iteração com k=$k foi de $silhouette")
  end analisadadosesalva

end Fluxo
