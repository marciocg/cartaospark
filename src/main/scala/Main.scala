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
  mode
}
import org.apache.spark.sql.Column

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
    // df.select(renomearColunas: _*).show()           //transforma Lista em VarArgs scala 2
    // df.select(renomearColunas*).show()           //transforma Lista em VarArgs scala 3
    df.select(df.columns.map(c => col(c).as(c.toLowerCase()))*).show()

    // tá faltando converter as colunas de data pra tipo data -> estudar as funcoes!
    val dados = df.select(renomearColunas*)
    val comDiferenca = dados
      .withColumn("diff", date_diff(col("dataVencimento"), col("dataEmissao")))
    // comDiferenca.show()
    comDiferenca.describe().show()
    comDiferenca.agg(median(col("limite")), mode(col("limite"))).show()

    spark.stop()
