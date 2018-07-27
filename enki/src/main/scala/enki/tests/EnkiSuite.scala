package enki
package tests

import java.nio.file.Files

import org.apache.spark.sql.SparkSession

trait EnkiSuite extends Defaults with ImplicitConversions {
  protected def createSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("enki-test")
      .master(s"local[${Runtime.getRuntime.availableProcessors}]")
      .config("spark.sql.shuffle.partitions", Runtime.getRuntime.availableProcessors)
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toUri.toString)
      .getOrCreate()
  }

  protected implicit lazy val sparkSession: SparkSession = createSparkSession()
}