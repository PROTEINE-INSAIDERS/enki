package enki.spark.plan

import enki.EnkiTestSuite
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf

class PlanTest extends EnkiTestSuite {
  "test" in {
    val parser = new SparkSqlParser(new SQLConf())
    val plan = parser.parsePlan("select * from table1")


    println(plan)
  }
}
