package enki.tests

import enki.{SparkAction, _}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.reflect.runtime.universe.typeOf

trait DatabaseState {

  protected def graph: ActionGraph

  def emulate: SparkAction[Unit] = session => {
    sources.foreach(action => prepareState(action, session))
  }

  def sources: Set[ReadAction[_]] = {
    val readers = graph.linearized.flatMap(name => stageReads(graph.actions(name))).map(action => (s"${action.schemaName}.${action.tableName}", action)).toSet
    val writers = graph.linearized.flatMap(name => stageWrites(graph.actions(name))).map(action => s"${action.schemaName}.${action.tableName}").toSet
    readers.filter { case (name, _) => !writers.contains(name) }.map { case (_, action) => action }
  }

  def prepareState: (ReadAction[_], SparkSession) => Unit =
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
