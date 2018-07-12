package enki

import enki._
import enki.tests.EnkiSuite
import org.apache.spark.sql.Row
import org.scalatest.{Matchers, WordSpec}
import cats._
import cats.implicits._

object Otest extends Database {
  override def schema: String = "schema"

  val a = read[Row]("test")

  val program = for {
    b <- persist[Row]("test2", a)
  } yield ()
}

class ProgramTest extends WordSpec with Matchers with EnkiSuite {
  "ddd" in {
  }
}
