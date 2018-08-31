package enki.testsuite

import java.nio.file.Files

import cats.implicits._
import enki._
import org.apache.spark.sql.{SaveMode, SparkSession}

trait EnkiSuite extends Defaults with ImplicitConversions {
  private def sources: ActionGraph => Set[ReadTableAction] = graph => {
    val readers = graph.analyze(action => stageReads(action, action => Set((action.toString, action))))
    val writers = graph.analyze(action => stageWrites(action, action => Set(action.toString)))
    readers.filter { case (name, _) => !writers.contains(name) }.map { case (_, action) => action }
  }

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
    sources(graph).foreach { action =>
      val tableName = s"${action.schemaName}.${action.tableName}"
      action match {
        case readDataset: ReadDatasetAction[t] =>
          if (!session.catalog.databaseExists(action.schemaName))
            session.sql(s"create database ${action.schemaName}")

          session.emptyDataset[t](readDataset.encoder)
            .write
            .mode(SaveMode.Overwrite)
            .saveAsTable(tableName)

        case _: ReadDataFrameAction =>
          throw new UnsupportedOperationException(s"Can not create empty table $tableName of generic type Row.")
      }
    }
  }
}