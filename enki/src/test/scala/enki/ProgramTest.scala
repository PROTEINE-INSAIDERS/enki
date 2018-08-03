package enki

import cats.implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.types.{DecimalType, StructField, StructType}
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._

case class TestData(a: BigDecimal)

class ProgramTest extends EnkiTestSuite {
  "buildActionGraph" should {
    "detect dependencies" in {

      import sparkSession.implicits._

      val p: Program[Stage[Unit]] = for {
        a <- persist("default", "a", dataset(Seq(1)), strict = false, None)
        b <- persist("default", "b", a, strict = false, None)
        c <- persist("default", "c", a, strict = false, None)
        d <- persist("default", "d", (c, b) mapN { (_, _) => sparkSession.emptyDataset[Int] }, strict = false, None)
      } yield ().pure[Stage]

      val g = buildActionGraph("root", p)

      g.graph.edges.toOuter.toList.sortBy { case DiEdge(from, to) => (from, to) } shouldBe Seq(
        "default.b" ~> "default.a",
        "default.c" ~> "default.a",
        "default.d" ~> "default.b",
        "default.d" ~> "default.c")
    }

    "ignore empty stages" in {
      val p: Program[Stage[Unit]] = for {
        a <- persist("default", "a", dataset(Seq(1)), strict = false, None)
      } yield ().pure[Stage]

      val g = buildActionGraph("root", p)
      g.actions.keys.toSeq shouldBe Seq("default.a")
    }
  }
  /*

"test_test" in {



  val customSchema = StructType(Seq(StructField(
    name = "a",
    dataType = DecimalType(38, 12)
  )))

  val aa = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], customSchema )
  val encoder =  ExpressionEncoder[TestData]().copy(schema = customSchema )
  println(encoder)


  aa.write.saveAsTable("testTable")

   val table = sparkSession.table("testTable")

  //println(s"=== pure schema: ${table.schema}")

  val converted = table.as[TestData](encoder)

  println(s"=== converted schema: ${converted.schema}")

  // b.show()

}
*/
}
