package enki
package tests

import java.nio.file.Files

import cats.implicits._
import org.apache.spark.sql.SparkSession


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

  def createEmptySources(graph: ActionGraph, session: SparkSession): Unit = {
    sources(graph).foreach {
      case action: ReadDatasetAction[t] =>
        if (!session.catalog.databaseExists(action.schemaName)) session.sql(s"create database ${action.schemaName}")
        session.emptyDataset[t](action.encoder).write.saveAsTable(action.toString)

      case _ => throw new UnsupportedOperationException("Can not create empty table from DataFrame.")
    }
  }

  def sources: ActionGraph => Set[ReadTableAction] = graph => {
    val readers = graph.analyze(action => stageReads(action, action => Set((action.toString, action))))
    val writers = graph.analyze(action => stageWrites(action, action => Set(action.toString)))
    readers.filter { case (name, _) => !writers.contains(name) }.map { case (_, action) => action }
  }
}