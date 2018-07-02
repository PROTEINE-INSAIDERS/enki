package enki

import cats._
import cats.implicits._
import enki.readers.default._
import enki.tests.EnkiSuite
import enki.writers.default._
import org.apache.spark.sql._
import org.scalatest.{Matchers, WordSpec}

import scala.reflect.io.Path

class TestTest extends WordSpec with Matchers with EnkiSuite {
  "stages" in {
/*
    val a = read[Row]('testTable1)

    val s1 = stage('s1, a)

    val b = (d1: Dataset[Row], d2: Dataset[Row]) => {
      d1.crossJoin(d2)
    }

    val s2 = stage('s2, (s1, s1) mapN b)

    val s3 = stage('s3, (s1, s1) mapN b)
*/
  }

  "aaa" in {
/* TODO: отклчено, пока не доделана работа с подпрограммами.
    val a = read[Row]('testTable1)
    val b = read[(Int, Int)]('sourceB)

    val c: Program[DataFrame] = (a, b, session) mapN { (dsa, dsb, s) =>
      import s.implicits._
      dsa.as("a").join(dsb.as("b"), $"a.c" === $"b._1")
    }

    val d = write[Row]('testDst, c)

    val emptySource = new EmptyReader with SchemaFromSource {
      override def root: Path = Path("/schemas")
    }

    val m = readerMapper(_ => emptySource)

    val ee = evaluator compose m

    val res = d foldMap ee

    sparkSession.sql("select * from testDst").show
    */
  }
}
