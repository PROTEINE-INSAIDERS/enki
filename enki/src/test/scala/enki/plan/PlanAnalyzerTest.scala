package enki.plan

import enki.EnkiTestSuite
import enki.default._
import org.apache.spark.sql.catalyst.TableIdentifier

class PlanAnalyzerTest extends EnkiTestSuite {
  "mapTableNames" should {
    "support CTE" in {
      val sql = "with a as (select * from a), b as (select * from a) select * from a, b, c"
      val plan = sparkSession.sessionState.sqlParser.parsePlan(sql)
      val analyzer = new PlanAnalyzer {}
      val mapped = analyzer.mapTableNames(plan, {
        case TableIdentifier("a", None) => TableIdentifier("a1", None)
        case TableIdentifier("c", None) => TableIdentifier("c1", None)
        case ti => ti
      })
      val expected = sparkSession.sessionState.sqlParser.parsePlan(
        "with a as (select * from a1), b as (select * from a) select * from a, b, c1")
      mapped shouldBe expected
    }
  }
}
