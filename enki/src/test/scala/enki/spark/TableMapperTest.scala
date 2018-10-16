package enki
package spark

import cats.implicits._
import enki.default._
import org.apache.spark.sql.catalyst.TableIdentifier

class TableMapperTest extends EnkiTestSuite {
  "tableMapper" should {
    "map table names in select query" in {
      val mapper = TableMapper {
        case TableIdentifier("table1", Some("default")) => TableIdentifier("table2", Some("default"))
        case other => other
      }
      val readsAnalyzer = TableReads(Set(_))
      val reads = readsAnalyzer(mapper(SparkAlg.SqlOp("select * from default.table1"))).getConst
      reads shouldBe Set(ReadDataFrameAction("default", "table2"))
    }
  }
}
