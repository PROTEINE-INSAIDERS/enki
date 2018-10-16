package enki

import org.apache.spark.sql.catalyst.parser.AbstractSqlParser
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf

class VariableSubstitution  extends EnkiTestSuite {
  "test" in {
    val conf = new SQLConf()
    conf.setConfString("xxx", "\"yoba\"")
   def parser: AbstractSqlParser = new SparkSqlParser(conf)
   val res = parser.parsePlan("select * from testTable where a = ${xxx}")

  }

}
