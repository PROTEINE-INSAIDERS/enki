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
      dataFrame: DataFrame => action.write(action.writerSettings, dataFrame)

    case action: WriteDatasetAction[t] => _: Environment =>
      dataset: Dataset[t] =>
        if (action.strict) {
          action.write(
            action.writerSettings,
            dataset.select(action.encoder.schema.map(f => dataset(f.name)): _*).as[t](action.encoder))
        } else {
          action.write(action.writerSettings, dataset)
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
