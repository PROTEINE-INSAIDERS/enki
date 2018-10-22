package enki
package spark

import cats.free.Free
import enki.default._
import org.apache.spark.sql.Row

class SparkHandlerTest extends EnkiTestSuite {
  "plan" should {
    "handle select statemen" in {
      val handler = new SparkHandler()
      val plan = sparkSession.sessionState.sqlParser.parsePlan("select 1 as a, '2' as b")
      val res = handler(SparkAlg.PlanOp(plan))  (Environment(sparkSession))(identity _)
      res.collect() shouldBe Array(Row(1, "2"))
    }
  }
}
