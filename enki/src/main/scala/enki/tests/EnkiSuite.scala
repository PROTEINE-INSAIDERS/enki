package enki
package tests

import java.nio.file.Files

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Row, SparkSession}

import scala.reflect.runtime.universe.typeOf


trait EnkiSuite extends Defaults with ImplicitConversions with TestDataFrameModule {
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

  def sources: ActionGraph => Set[ReadAction[_]] = graph => {
    val readers = graph.actions.flatMap { case (_, action) => stageReads(action) }.map(action => (s"${action.schemaName}.${action.tableName}", action)).toSet
    val writers = graph.actions.flatMap { case (_, action) => stageWrites(action) }.map(action => s"${action.schemaName}.${action.tableName}").toSet
    readers.filter { case (name, _) => !writers.contains(name) }.map { case (_, action) => action }
  }

  def createEmptySource(action: ReadAction[_], session: SparkSession): Unit = {
    case (action: ReadAction[t], session: SparkSession) =>
      if (!session.catalog.databaseExists(action.schemaName)) session.sql(s"create database ${action.schemaName}")
      session.emptyDataFrame.cast[t](action.strict)(action.tag).write.saveAsTable(s"${action.schemaName}.${action.tableName}")
    case _ => new Exception()
  }

}