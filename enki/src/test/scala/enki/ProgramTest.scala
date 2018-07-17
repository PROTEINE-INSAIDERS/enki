package enki

import enki._
import enki.tests.EnkiSuite
import org.apache.spark.sql.{Column, Dataset, Row}
import org.scalatest.{Matchers, WordSpec}
import cats._
import cats.implicits._

case class Class(f1: Int, f2: Int)

object Otest extends Database {
  override def schema: String = "schema"

  val a = read[Row]("test")

  val program = for {
    b <- persist[Row]("test2", a)
  } yield ()
}

class ProgramTest extends WordSpec with Matchers with EnkiSuite {
  implicit class MoreDatasetExtensions[T](dataset: Dataset[T])  {
    def apply(f : T => Any): Unit = {}
    def $(f : T => Any): Unit = {}
  }


  "ddd" in {
    import sparkSession.implicits._

    val aa = sparkSession.emptyDataset[Class]

    aa.$(_.f1)

  }
}
