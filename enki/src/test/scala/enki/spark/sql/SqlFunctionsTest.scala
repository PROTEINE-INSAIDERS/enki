package enki.spark.sql

import enki.EnkiTestSuite

class SqlFunctionsTest extends EnkiTestSuite {
  "typedNull" should {
    "compile for sting" in {
      "typedNull[String]" should compile
    }
  }
}