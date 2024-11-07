// package cartaospark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{
  lit,
  concat,
  count, count_distinct,
  expr,
  current_timestamp,
  col,
  date_diff,
  median,
  mode,
  to_date,
  year,
  max,
  date_format,
  to_timestamp,
  row_number
}
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode
// import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
// import org.apache.hadoop.fs.FileSystem
// import org.apache.hadoop.fs.LocalFileSystem
// import org.apache.hadoop.fs.RawLocalFileSystem

object Main:
  def main(args: Array[String]): Unit =
    val spark = SparkSession
      .builder()
      .appName { "cartao-spark" }
      .master("local[*]")
      .getOrCreate()
    /*
    val df = spark    //.sparkContext.hadoopConfiguration.setClass("fs.file.impl", BareLocalFileSystem, FileSystem)          //configura um fs hadoop fake pra fazer write do parquet
      .read
      .option("header", value = true)
      .option("inferSchema", value = true)
      // .option("dateformat", "MM/yyyy")
      .option("timestampFormat", value = "yyyy-MM-dd HH:mm:ss")
      .option("sep", value = "|")
      .csv("data/agosto.csv")

      // df.write.mode(SaveMode.Overwrite).parquet("data/agosto.parquet")
     */
    val df = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      // .option("dateformat", "MM/yyyy")
      .option("timestampFormat", value = "yyyy-MM-dd HH:mm:ss")
      .option("sep", value = "|")
      .parquet("data/agosto.parquet")

    val renomearColunas = List(
      col("000").as("Quantidade"),
      col("002").as("Data"),
      col("006").as("Hora"),
      col("035").as("Cartao"),
      col("068").as("Logo"),
      col("087").as("Action Reason"),
      col("089").as("Reversal Reason"),
      col("207").as("Agencia"),
      col("270").as("Resposta Senha"),
      col("388").as("Resposta"),
      col("332").as("Processing Code"),
      col("545").as("MTI"),
      col("536").as("Trans Type"),
      col("557").as("Blq Cta"),
      col("558").as("Blq Plst"),
      col("368").as("Entry Mode"),
      col("2501").as("Tokenizada"),
      col("067").as("Bandeira"),
      col("1902").as("Tam BIN"),
      col("363").as("MCC"),
      col("372").as("Tam Adq"),
      col("374").as("Cod Adq"),
      col("457").as("Moeda"),
      col("610").as("Combo"),
      col("611").as("Ind Filtro"),
      col("612").as("Regra Filtro"),
      col("630").as("Tip Compra"),
      col("1715").as("Vpin Exec"),
      col("595").as("Vpin Ok"),
      col("596").as("Vpin Acao"),
      col("598").as("Vpin Ret"),
      col("605").as("Aux 1"),
      col("607").as("Aux 2"),
      col("617").as("Aux 4"),
      col("1880").as("Vipc851 Call"),
      col("1881").as("Vipc851 Ret"),
      col("1885").as("Vipc851 Loc"),
      col("2024").as("B123 ID68 TAG03"),
      col("1701").as("BB Transacao"),
      col("1661").as("Ind Deb"),
      col("702").as("B44 cvv resp"),
      col("707").as("B44 cvv2 resp"),
      col("708").as("B44 cavv resp"),
      col("670").as("Ret Code"),
      col("671").as("Tipo Trans"),
      col("672").as("Flag CVV"),
      col("673").as("Flag CVC"),
      col("674").as("Flag Pin"),
      col("675").as("Flag Seg Adc"),
      col("627").as("Found"),
      col("628").as("Mens 120"),
      col("629").as("Opcao"),
      col("602").as("Ret Espec"),
      col("550").as("Loc PrePG"),
      col("393").as("C Accpt Term"),
      col("401").as("C Accpt ID"),
      col("366").as("Country Code"),
      col("063").as("Card Type"),
      col("065").as("TC"),
      col("062").as("Request Type ID"),
      col("090").as("Auth Code"),
      col("035B").as("BIN"),
      col("510").as("VIPF310 Status"),
      col("609").as("Ind Empresarial"),
      col("1626").as("MCI"),
      col("100").as("TOTAL-SALES-AMT"),
      col("999").as("Sexo")
    )
    //val dados = df.select(renomearColunas*).show()

    val mci = df("1626").as("mci")
    val cartao = df("035").as("cartao")

    val grupo = List(cartao, mci) // grupo é argumento varargs quando usa "*" ao lado da variável

    //os 2 groupBy abaixo dão o mesmo resultado
/*     df.groupBy(grupo*).agg(count(mci).as("qtd_mci_cartao")).show()
    df.groupBy(grupo*).agg(Map(
      "1626" -> "count"
    )).show()
 */
    // conta quantidade de mci diferente para cada plástico mascarado
    df.groupBy(cartao)
      .agg(count_distinct(mci).as("qtd_mci_por_cartao"))
      .sort(col("qtd_mci_por_cartao").desc)
      .show()

    df.select(mci).filter(cartao === "48546412XXXX3359").distinct().show(26)
    df.select(mci).filter(cartao === "49845312XXXX4504").distinct().show()

    /*
    // df.select("Issuing Bank", "Card Number").show()
    val emissor = df("Issuing Bank")

    val cartao = df(
      "Card Number"
    ) // criou uma coluna q serve de argumento pra .select
    // val cartaoSemDigito = cartao / 10
    // val cartaoSemDigito = (cartao / 10).as(("Cartão sem Dígito").getBytes("ISO-8859-1").mkString)
    val cartaoSemDigito = (cartao / 10).as("Cartao sem Digito")
    val cartaoSemDigitoStr = cartaoSemDigito.cast(StringType)

    val litColuna = lit(10)
    val cartaoSemDigitoStrPlastico = concat(cartaoSemDigitoStr, lit("Plastico"))

    // df.select(cartao, cartaoSemDigito, cartaoSemDigitoStr, cartaoSemDigitoStrPlastico)
    //   .filter(cartaoSemDigito > 3.096594393084E12)
    //   .filter(cartao > cartaoSemDigito)
    //   // .filter(cartao === cartaoSemDigito)        // usar === para igualdade de colunas
    //   .show(truncate = false)

    val tsColunaExpr: Column = expr(
      "cast(current_timestamp() as string) as Expr_Timestamp"
    ) // expr avaliador de funcoes q retornam Column
    val tsColunaFunc = current_timestamp()
      .cast(StringType)
      .as("Funcao Timestamp") // a vantagem é validação do compilador

    /*
    df.select(tsColunaExpr, tsColunaFunc).show()
    df.selectExpr("cast(\'Card Number\' as string)", "\'CVV/CVV2\' + 1000", "current_timestamp()").show()   // nome de coluna com espaços ou métodos tem que escapar com plix
    df.createTempView("visao")
    spark.sql("select * from visao").show()
     */
    df.withColumnRenamed("Card Number", "plastico")
      .withColumnRenamed("Issuing Bank", "emissor")

    // dá pra usar a funcao col() e também `import spark.implicits.*` e $"Issuing Bank"

    // aqui foi dificil mas resolveu uma coluna, se todas são mesmo padrao dá pra usar no .option do .spark.read
    /*  val testeDateFormatStr = date_format(to_timestamp(col("Issue Date"), "MM/yyyy"), "MM/yyyy") //devolve string
    val testeDateFormat = to_date(to_timestamp(col("Issue Date"), "MM/yyyy"))     //devpolve date
    df.select(testeDateFormat).printSchema()
    df.select(testeDateFormat).show()
     */

    val renomearColunas = List(
    col("000").as("Quantidade")
col("002").as("Data")
col("006").as("Hora")
col("035").as("Cartao")
col("068").as("Logo")
col("087").as("Action Reason")
col("089").as("Reversal Reason")
col("207").as("Agencia")
col("270").as("Resposta Senha")
col("388").as("Resposta")
col("332").as("Processing Code")
col("545").as("MTI")
col("536").as("Trans Type")
col("557").as("Blq Cta")
col("558").as("Blq Plst")
col("368").as("Entry Mode")
col("2501").as("Tokenizada")
col("067").as("Bandeira")
col("1902").as("Tam BIN")
col("363").as("MCC")
col("372").as("Tam Adq")
col("374").as("Cod Adq")
col("457").as("Moeda")
col("610").as("Combo")
col("611").as("Ind Filtro")
col("612").as("Regra Filtro")
col("630").as("Tip Compra")
col("1715").as("Vpin Exec")
col("595").as("Vpin Ok")
col("596").as("Vpin Acao")
col("598").as("Vpin Ret")
col("605").as("Aux 1")
col("607").as("Aux 2")
col("617").as("Aux 4")
col("1880").as("Vipc851 Call")
col("1881").as("Vipc851 Ret")
col("1885").as("Vipc851 Loc")
col("2024").as("B123 ID68 TAG03")
col("1701").as("BB Transacao")
col("1661").as("Ind Deb")
col("702").as("B44 cvv resp")
col("707").as("B44 cvv2 resp")
col("708").as("B44 cavv resp")
col("670").as("Ret Code")
col("671").as("Tipo Trans")
col("672").as("Flag CVV")
col("673").as("Flag CVC")
col("674").as("Flag Pin")
col("675").as("Flag Seg Adc")
col("627").as("Found")
col("628").as("Mens 120")
col("629").as("Opcao")
col("602").as("Ret Espec")
col("550").as("Loc PrePG")
col("393").as("C Accpt Term")
col("401").as("C Accpt ID")
col("366").as("Country Code")
col("063").as("Card Type")
col("065").as("TC")
col("062").as("Request Type ID")
col("090").as("Auth Code")
col("035B").as("BIN")
col("510").as("VIPF310 Status")
col("609").as("Ind Empresarial")
col("1626").as("MCI")
col("100").as("TOTAL-SALES-AMT")
col("999").as("Sexo")
    )
    val dados = df.select(renomearColunas*)
    val dtEmissao = dados("dataEmissao")
    val anoEmissao = year(dtEmissao).as("ano")

    // df.select(renomearColunas: _*).show()           //transforma Lista em VarArgs scala 2
    // df.select(renomearColunas*).show()           //transforma Lista em VarArgs scala 3
    // df.select(df.columns.map(c => col(c).as(c.toLowerCase()))*).show()  // coloca todas colunas em minúsculo

    val comDiferenca = dados
      .withColumn(
        "diff",
        date_diff(col("dataVencimento"), col("dataEmissao"))
      ) // essa coluna é em dias
    // comDiferenca.describe().show()
    comDiferenca.show()
    comDiferenca
      .agg(
        median(col("limite").as("Mediana Limite")),
        mode(col("limite").as("Moda Limite"))
      )
      .show()

    dados
      .groupBy(anoEmissao)
      .agg(max("plastico"))
      // .filter(anoEmissao === 2017)
      .filter(anoEmissao < 2024)
      // .sort(anoEmissao.desc)          // nao entendi pq nao funciona
      .sort(col("ano").desc)
      .show()

    dados
      .groupBy(anoEmissao)
      .min("plastico")
      .show()

    // nao entendi window over direito, preciso estudar isso
    val windowMaiorlasticoDoAno = plasticoMaiorNumeracaoPorAno(anoEmissao, dados)
     */
    spark.stop()

  end main

end Main
