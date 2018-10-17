package enki
package testsuite

import java.nio.file.Files

import enki.spark.plan.PlanAnalyzer
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}


trait EnkiSuite extends Defaults with ImplicitConversions {

  protected def createSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("enki-test")
      .master(s"local[${Runtime.getRuntime.availableProcessors}]")
      .config("spark.sql.shuffle.partitions", Runtime.getRuntime.availableProcessors.toLong)
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toUri.toString)
      .getOrCreate()
  }

  protected implicit lazy val sparkSession: SparkSession = createSparkSession()

  def createEmptyTable(createStatement: String)(implicit session: SparkSession): Unit = {
    val analyzer = new PlanAnalyzer {}
    analyzer.parsePlan(createStatement) match {
      case CreateTable(desc, _, _) =>
        desc.identifier.database.foreach(db => session.sql(s"create database if not exists $db"))
        sparkSession
          .createDataFrame(sparkSession.sparkContext.emptyRDD[Row], desc.schema)
          .write
          .mode(SaveMode.Ignore)
          .saveAsTable(desc.identifier.database.map(db => s"$db.").getOrElse("") + desc.identifier.table)
      case other => throw new Exception(s"$other is not a table.")
    }
  }
}