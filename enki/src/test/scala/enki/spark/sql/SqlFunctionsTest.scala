package enki.spark.sql

import enki.EnkiTestSuite

class SqlFunctionsTest extends EnkiTestSuite {
  "typedNull" should {
    "compile for sting" in {
      """
        |import enki.default._
        |typedNull[String]
      """.stripMargin should compile
    }
  }
}