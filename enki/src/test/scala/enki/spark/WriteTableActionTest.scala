package enki.spark

import enki.EnkiTestSuite
import enki.default._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier

class WriteTableActionTest extends EnkiTestSuite {
  "write" should {
    "be able to overwrite partition" in {
      import sparkSession.implicits._

      val ds = sparkSession.createDataset(Seq(("a", "a"), ("b", "b")))

      sparkSession.sql("create database if not exists test")

      val ws1 = WriterSettings(TableIdentifier("test", Some("test")))
        .setMode(SaveMode.Overwrite)
        .setPartition("_1" -> "a")

      val ws2 = WriterSettings(TableIdentifier("test", Some("test")))
        .setMode(SaveMode.Overwrite)
        .setPartition("_1" -> "b")

      val dc = new SparkHandler() {}

      dc.write(sparkSession, ws1, ds.where($"_1" === "a"))
      dc.write(sparkSession, ws2, ds.where($"_1" === "b"))
      dc.write(sparkSession, ws2, ds.where($"_1" === "b"))

      sparkSession.table("test.test").as[(String, String)].collect().sortBy(_._1) shouldBe Array(("a", "a"), ("b", "b"))
    }
  }
}
