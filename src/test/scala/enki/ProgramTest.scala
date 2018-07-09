package enki

import enki.tests.EnkiSuite
import org.apache.spark.sql.Row
import org.scalatest.{Matchers, WordSpec}
import cats._
import cats.implicits._

class ProgramTest extends WordSpec with Matchers with EnkiSuite {
  "ddd" in {
    implicit val db = new Database {
      override def schema: String = ""
    }

    val aaa = read[Row]("test")

    val bbb = for { a <- aaa.pure[Program] } yield ()
  }
}
