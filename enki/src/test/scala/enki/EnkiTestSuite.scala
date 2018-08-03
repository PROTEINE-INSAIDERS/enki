package enki

import enki.tests._
import org.apache.spark.sql.SparkSession
import org.scalatest.{Matchers, WordSpec}

trait EnkiTestSuite extends WordSpec with Matchers with EnkiSuite {
  override protected def createSparkSession(): SparkSession = {
    val session = super.createSparkSession()
    session.sparkContext.setLogLevel("ERROR")
    session
  }
}
