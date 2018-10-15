package enki.spark

import enki.EnkiTestSuite
import enki.default._
import org.apache.spark.sql._

class WriteTableActionTest extends EnkiTestSuite {
  "tstds" should {
    val a = sparkSession.sql("test")
      // создание датасета примерно так
    /*  при этом выполняем трансформацию плана.
      val qe = sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new Dataset[Row](sparkSession, qe, RowEncoder(qe.analyzed.schema))
    */
  }

  "write" should {
    "be able to overwrite partition" in {
      import sparkSession.implicits._

      val ds = sparkSession.createDataset(Seq(("a", "a"), ("b", "b")))

      sparkSession.sql("create database if not exists test")

      val writeTableAction = new WriteTableAction() {
        override def schemaName: String = "test"

        override def tableName: String = "test"
      }

      val ws1 = WriterSettings()
        .setMode(SaveMode.Overwrite)
        .setPartition("_1" -> "a")

      val ws2 = WriterSettings()
        .setMode(SaveMode.Overwrite)
        .setPartition("_1" -> "b")

      val dc = new SparkHandler() {}

      dc.write(sparkSession, "test", "test", ws1, ds.where($"_1" === "a"))
      dc.write(sparkSession, "test", "test", ws2, ds.where($"_1" === "b"))
      dc.write(sparkSession, "test", "test", ws2, ds.where($"_1" === "b"))

      sparkSession.table("test.test").as[(String, String)].collect().sortBy(_._1) shouldBe Array(("a", "a"), ("b", "b"))
    }
  }
}
