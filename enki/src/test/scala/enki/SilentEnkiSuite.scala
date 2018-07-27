package enki

import enki.tests._
import org.apache.spark.sql.SparkSession

trait SilentEnkiSuite extends EnkiSuite {
  override protected def createSparkSession(): SparkSession = {
    val session = super.createSparkSession()
    session.sparkContext.setLogLevel("ERROR")
    session
  }
}
