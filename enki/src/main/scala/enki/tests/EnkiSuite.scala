package enki
package tests

import java.nio.file.Files

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.reflect.runtime.universe.typeOf


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

  def createEmptySources: (ActionGraph, SparkSession) => Unit = (graph, session) => {
    sources(graph).foreach(action => createEmptySource(action, session))
  }

  private def sources: ActionGraph => Set[ReadAction[_]] = graph => {
    val readers = graph.actions.flatMap { case (_, action) => stageReads(action) }.map(action => (s"${action.schemaName}.${action.tableName}", action)).toSet
    val writers = graph.actions.flatMap { case (_, action) => stageWrites(action) }.map(action => s"${action.schemaName}.${action.tableName}").toSet
    readers.filter { case (name, _) => !writers.contains(name) }.map { case (_, action) => action }
  }

  private def createEmptySource: (ReadAction[_], SparkSession) => Unit =
    (action, session) =>
      action match {
        case readAction: ReadAction[t] =>
          if (!session.catalog.databaseExists(readAction.schemaName)) session.sql(s"create database ${readAction.schemaName}")

          if (action.tag.tpe == typeOf[Row]) {
            if (action.strict) {
              throw new Exception("Unable to restrict schema for generic type Row.")
            }
            session.emptyDataFrame.asInstanceOf[Dataset[t]].write.saveAsTable(s"${readAction.schemaName}.${readAction.tableName}")
          } else {
            val encoder = ExpressionEncoder[t]()(readAction.tag)
            session.emptyDataset[t](encoder).write.saveAsTable(s"${readAction.schemaName}.${readAction.tableName}")
          }
      }


}