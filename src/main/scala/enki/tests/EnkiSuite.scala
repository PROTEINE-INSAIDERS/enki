package enki.tests

import java.nio.file.Files

import enki.program.Compiler
import enki.syntax.AllSyntax
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait EnkiSuite extends AllSyntax with Compiler {
  protected implicit lazy val sparkSession: SparkSession = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    SparkSession
      .builder()
      .appName("enki-test")
      .master("local")
      .config("spark.sql.shuffle.partitions", Runtime.getRuntime.availableProcessors)
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toUri.toString)
      .getOrCreate()
  }
}
