package enki

import cats._
import cats.free.FreeApplicative
import cats.free.FreeApplicative._
import cats.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

trait StageModule {


  type Stage[A] = FreeApplicative[StageAction, A]

  def dataFrame(rows: Seq[Row], schema: StructType): Stage[DataFrame] =
    lift[StageAction, DataFrame](DataFrameAction(rows, schema))

  def dataset[T](data: Seq[T], encoder: Encoder[T]): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](DatasetAction(data, encoder))

  def emptyStage: Stage[Unit] = pure(())

  def readDataFrame(
                     schemaName: String,
                     tableName: String
                   ): Stage[DataFrame] =
    lift[StageAction, DataFrame](ReadDataFrameAction(schemaName, tableName))

  def readDataset[T](
                      schemaName: String,
                      tableName: String,
                      encoder: Encoder[T],
                      strict: Boolean
                    ): Stage[Dataset[T]] =
    lift[StageAction, Dataset[T]](ReadDatasetAction(schemaName, tableName, encoder, strict))

  def writeDataFrame(
                      schemaName: String,
                      tableName: String,
                      saveMode: Option[SaveMode]
                    ): Stage[DataFrame => Unit] =
    lift[StageAction, DataFrame => Unit](WriteDataFrameAction(schemaName, tableName, saveMode))

  def writeDataset[T](
                       schemaName: String,
                       tableName: String,
                       encoder: Encoder[T],
                       strict: Boolean,
                       saveMode: Option[SaveMode]
                     ): Stage[Dataset[T] => Unit] =
    lift[StageAction, Dataset[T] => Unit](WriteDatasetAction(schemaName, tableName, encoder, strict, saveMode))

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

    case arg@StringArgument(name) => env: Environment =>
      env.parameters.get(name) match {
        case Some(StringValue(str)) => str
        case Some(other) =>
          throw new Exception(s"Parameter's value $name has wrong type: required ${arg.typeName} actual ${other.typeName}.")
        case None => throw new Exception(s"Parameter $name not found.")
      }

    case arg@IntegerArgument(name) => env: Environment =>
      env.parameters.get(name) match {
        case Some(IntegerValue(int)) => int
        case Some(other) =>
          throw new Exception(s"Parameter's value $name has wrong type: required ${arg.typeName} actual ${other.typeName}.")
        case None => throw new Exception(s"Parameter $name not found.")
      }
  }

  def stageArguments[M: Monoid](stage: Stage[_], f: ArgumentAction => M): M = {
    stage.analyze(λ[StageAction ~> λ[α => M]] {
      case r: ArgumentAction => f(r)
      case _ => implicitly[Monoid[M]].empty
    })
  }

  def stageReads[M: Monoid](stage: Stage[_], f: ReadTableAction => M): M = {
    stage.analyze(λ[StageAction ~> λ[α => M]] {
      case r: ReadTableAction => f(r)
      case _ => implicitly[Monoid[M]].empty
    })
  }

  def stageWrites[M: Monoid](stage: Stage[_], f: WriteTableAction => M): M = {
    stage.analyze(λ[StageAction ~> λ[α => M]] {
      case w: WriteTableAction => f(w)
      case _ => implicitly[Monoid[M]].empty
    })
  }

  def stageNonEmpty(stage: Stage[_]): Boolean = {
    stage.analyze(λ[StageAction ~> λ[α => Option[Unit]]] {
      case _ => Some(())
    }).nonEmpty
  }
}