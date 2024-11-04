// For more information on writing tests, see
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.{LongType, DateType}
import org.apache.spark.sql.Row
import java.sql.Date
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.functions.year

// https://scalameta.org/munit/docs/getting-started.html
class MySuite extends munit.FunSuite {

  private val spark = SparkSession
    .builder()
    .appName("Teste")
    .master("local[*]")
    .getOrCreate()

  // criando a tabela igual ao CSV, mas só com as colunas que quero testar DDL
  private val schema = StructType(
    Seq(
      StructField("dataEmissao", DateType, nullable = false),
      StructField("plastico", LongType, nullable = false)
    )
  )

  // inserindo dados na tabela pra teste DML
  test("Testa método no object Main") {
    val linTeste = Seq(
      Row(Date.valueOf("2020-01-31"), 4984498449844984L),
      Row(Date.valueOf("2021-02-28"), 5067506750675067L),
      Row(Date.valueOf("2021-03-30"), 6550655065506550L)
    )
    val expected = Seq(
      Row(Date.valueOf("2021-02-28"), 5067506750675067L, 1),    // o '1' é da coluna `rank`
      Row(Date.valueOf("2020-01-31"), 4984498449844984L, 1),
      //Row(Date.valueOf("2021-03-30"), 6550655065506550L)
    )

    implicit val encoder: Encoder[Row] = Encoders.row(schema)  // o método createDataSet precisa de um encoder implícito

    val dadosTeste = spark.createDataset(linTeste)
    val dtEmissao = dadosTeste("dataEmissao")
    val anoEmissao = year(dtEmissao).as("ano")
    val result =
      Main.plasticoMaiorNumeracaoPorAno(anoEmissao, dadosTeste).collect().toSeq

    assertEquals(result, expected)      //pode passar um Array[T] onde se espera um Seq[T]

  }

  test("example test that succeeds") {
    val obtained = 42
    val expected = 42
    assertEquals(obtained, expected)
  }

  test("Teste função soma(2,3) retornando 5") {
    assertEquals(Main.soma(2, 3), 5)
  }

}
