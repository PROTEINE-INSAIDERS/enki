package enki.stage

import cats._
import enki._
import org.apache.spark.sql._

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
      dataFrame: DataFrame => {
        val writer = dataFrame.write
        action.saveMode.foreach(writer.mode)
        writer.saveAsTable(s"${action.schemaName}.${action.tableName}")
      }

    case action: WriteDatasetAction[t] => _: Environment =>
      dataset: Dataset[t] => {
        val resticted = if (action.strict) {
          dataset.select(action.encoder.schema.map(f => dataset(f.name)): _*)
        } else {
          dataset
        }
        val writer = resticted.as[t](action.encoder).write //TODO: возможно в некоторых случаях этот каст лишний.
        action.saveMode.foreach(writer.mode)
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
