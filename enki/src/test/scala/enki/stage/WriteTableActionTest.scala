package enki.stage

import cats.implicits._
import enki._
import org.apache.spark.sql._

class WriteTableActionTest extends EnkiTestSuite {
  "write" should {
    "be able to overwrite partition" in {
      import sparkSession.implicits._

      def part1[F[_]](implicit writer: enki.DataFrameWriter[F]): writer.FS[Unit] =
        writer.partition(Seq("_1" -> "a")) *> writer.mode(SaveMode.Overwrite)

      def part2[F[_]](implicit writer: enki.DataFrameWriter[F]): writer.FS[Unit] =
        writer.partition(Seq("_1" -> "b")) *> writer.mode(SaveMode.Overwrite)


      val ds = sparkSession.createDataset(Seq(("a", "a"), ("b", "b")))

      sparkSession.sql("create database if not exists test")

      val writeTableAction = new WriteTableAction() {
        override def schemaName: String = "test"

        override def tableName: String = "test"
      }

      writeTableAction.write(part1, ds.where($"_1" === "a"))
      writeTableAction.write(part2, ds.where($"_1" === "b"))
      writeTableAction.write(part2, ds.where($"_1" === "b"))

      sparkSession.table("test.test").as[(String, String)].collect().sortBy(_._1) shouldBe Array(("a", "a"), ("b", "b"))
    }
  }
}
