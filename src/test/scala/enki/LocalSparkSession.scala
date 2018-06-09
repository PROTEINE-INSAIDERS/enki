package enki

import java.nio.file.Files

import org.apache.log4j._
import org.apache.spark.sql._

object LocalSparkSession {
  lazy val session: SparkSession = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    SparkSession
      .builder()
      .appName("test")
      .master("local")
      .config("spark.sql.shuffle.partitions", 4)
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toUri.toString)
      .getOrCreate()
  }
}
