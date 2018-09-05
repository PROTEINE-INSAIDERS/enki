package enki.stage

import cats._
import enki._
import org.apache.spark.sql._
import freestyle.free._
import freestyle.free.implicits._

import scala.collection.JavaConversions._

trait Compilers {
  def stageCompiler: StageAction ~> SparkAction = λ[StageAction ~> SparkAction] {
    case action: DataFrameAction => env: Environment =>
      env.session.createDataFrame(action.rows, action.schema)

    case action: DatasetAction[t] => env: Environment =>
      env.session.createDataset[t](action.data)(action.encoder)

    case action: ReadDataFrameAction => env: Environment =>
      env.session.table(s"${action.schemaName}.${action.tableName}")

    case action: ReadDatasetAction[t] => env: Environment => {
      val dataframe = env.session.table(s"${action.schemaName}.${action.tableName}")
      val restricted = if (action.strict) {
        dataframe.select(action.encoder.schema.map(f => dataframe(f.name)): _*)
      } else {
        dataframe
      }
      restricted.as[t](action.encoder)
    }

    case action: WriteDataFrameAction => _: Environment =>
      dataFrame: DataFrame => imply(new DataFrameWriterConfigurator[Row]()) {
        val state = action.writerSettings.interpret[DataFrameWriterState[Row, ?]]
        val writer = state.runS(dataFrame.write).value
        writer.saveAsTable(s"${action.schemaName}.${action.tableName}")
      }

    case action: WriteDatasetAction[t] => _: Environment =>
      dataset: Dataset[t] => imply(new DataFrameWriterConfigurator[t]()) {
        val resticted = if (action.strict) {
          dataset.select(action.encoder.schema.map(f => dataset(f.name)): _*)
        } else {
          dataset
        }
        val state = action.writerSettings.interpret[DataFrameWriterState[t, ?]]
        val writer = state.runS(resticted.as[t](action.encoder).write).value //TODO: возможно в некоторых случаях этот каст лишний.
        writer.saveAsTable(s"${action.schemaName}.${action.tableName}")
      }

    case arg: StringArgumentAction => env: Environment => arg.fromParameterMap(env.parameters)

    case arg: IntegerArgumentAction => env: Environment => arg.fromParameterMap(env.parameters)
  }

  def tableNameMapper(f: (String, String) => (String, String)): StageAction ~> StageAction = λ[StageAction ~> StageAction] {
    case action: ReadDataFrameAction =>
      val (schemaName, tableName) = f(action.schemaName, action.tableName)
      action.copy(schemaName = schemaName, tableName = tableName)
    case action: ReadDatasetAction[_] =>
      val (schemaName, tableName) = f(action.schemaName, action.tableName)
      action.copy(schemaName = schemaName, tableName = tableName)
    case action: WriteDataFrameAction =>
      val (schemaName, tableName) = f(action.schemaName, action.tableName)
      action.copy(schemaName = schemaName, tableName = tableName)
    case action: WriteDatasetAction[_] =>
      val (schemaName, tableName) = f(action.schemaName, action.tableName)
      action.copy(schemaName = schemaName, tableName = tableName)
    case other => other
  }
}
