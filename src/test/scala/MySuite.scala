// For more information on writing tests, see
// https://scalameta.org/munit/docs/getting-started.html
class MySuite extends munit.FunSuite {
  test("example test that succeeds") {
    val obtained = 42
    val expected = 42
    assertEquals(obtained, expected)
  }

  test("Teste função soma(2,3) retornando 5") {
    assertEquals(Main.soma(2,3), 5)
  }
  
}
