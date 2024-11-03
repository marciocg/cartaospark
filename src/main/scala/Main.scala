// package cartaospark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{
  lit,
  concat,
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

object Main:
  def main(args: Array[String]): Unit =
    val spark = SparkSession
      .builder()
      .appName { "cartao-spark" }
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .option("dateformat", "MM/yyyy")
      .csv("data/1000CC.csv")

    // df.show()
    df.printSchema()

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
      col("Card Number").as("plastico"),
      col("Issuing Bank").as("emissor"),
      col("Card Type Full Name").as("bandeira"),
      col("Card Holder's Name").as("portador"),
      col("CVV/CVV2").as("codigoSeguranca"),
      col("Issue Date").as("dataEmissao"),
      col("Expiry Date").as("dataVencimento"),
      col("Billing Date").as("dataCobranca"),
      col("Card PIN").as("senha"),
      col("Credit Limit").as("limite")
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
    val janela = Window.partitionBy(anoEmissao).orderBy(col("plastico"))
    dados
      .withColumn("rank", row_number().over(janela))
      .filter(col("rank") === 2)
      .sort(col("plastico").desc)
      // .show()
      .explain(extended = true)

    spark.stop()
