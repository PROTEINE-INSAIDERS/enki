package enki
package stage

import cats.data.State
import freestyle.free._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

sealed trait StageAction[T]

final case class DataFrameAction(rows: Seq[Row], schema: StructType) extends StageAction[DataFrame]

final case class DatasetAction[T](data: Seq[T], encoder: Encoder[T]) extends StageAction[Dataset[T]]

trait TableAction {
  def schemaName: String

  def tableName: String

  override def toString: String = s"$schemaName.$tableName"
}

trait ReadTableAction extends TableAction

//TODO: ReadAction и WriteAction могут содержать много настроечных параметров. Необходимо вынести их в ReaderSettings и
// WriterSettings, чтобы в дальнейшем уменьнить количество рефакторингов при добавлении новых параметров.
final case class ReadDataFrameAction(
                                      schemaName: String,
                                      tableName: String
                                    ) extends StageAction[DataFrame] with ReadTableAction

final case class ReadDatasetAction[T](
                                       schemaName: String,
                                       tableName: String,
                                       encoder: Encoder[T],
                                       strict: Boolean
                                     ) extends StageAction[Dataset[T]] with ReadTableAction

trait WriteTableAction extends TableAction {

  //TODO: Временное решение. После перехода на freestyle этот метод будет в интерпретаторе.
  //TODO:
  private[enki] def write[T](writerSettings: FreeS.Par[DataFrameWriter.Op, Unit], dataset: Dataset[T]): Unit =
    imply(new DataFrameWriterSettingHandler[T]()) {

      val state = writerSettings.interpret[State[DataFrameWriterSettings[T], ?]]
      val settings = state.runS(DataFrameWriterSettings()).value
      val session = dataset.sparkSession
      (session.catalog.tableExists(schemaName, tableName), settings.partition) match {
        case (true, partition) if partition.nonEmpty =>
          dataset
            .where(partition map (p => dataset(p._1) === lit(p._2)) reduce (_ and _))
            .drop(partition.map(_._1): _*)
            .createTempView(s"tmp_$tableName")
          try {
            val partitionStr = partition.map(a => s"${a._1} = '${a._2}'").mkString(", ")
            session.sql(s"insert ${if (settings.overwrite) "overwrite" else ""} table $schemaName.$tableName partition($partitionStr) select * from tmp_$tableName")
            ()
          } finally {
            session.catalog.dropTempView(s"tmp_$tableName")
            ()
          }
        case (false, partition) if partition.nonEmpty =>
          settings.configure(dataset.write) //TODO: filter records by partition.
            .partitionBy(partition.map(_._1): _*)
            .saveAsTable(s"$schemaName.$tableName")
        case _ => settings.configure(dataset.write).saveAsTable(s"$schemaName.$tableName")
      }
    }
}

final case class WriteDataFrameAction(
                                       schemaName: String,
                                       tableName: String,
                                       writerSettings: FreeS.Par[DataFrameWriter.Op, Unit]
                                     ) extends StageAction[DataFrame => Unit] with WriteTableAction

final case class WriteDatasetAction[T](
                                        schemaName: String,
                                        tableName: String,
                                        encoder: Encoder[T],
                                        strict: Boolean,
                                        writerSettings: FreeS.Par[DataFrameWriter.Op, Unit]
                                      ) extends StageAction[Dataset[T] => Unit] with WriteTableAction

sealed trait ArgumentAction {
  def name: String

  def description: String

  def dataType: DataType

  private[enki] def defaultStringValue: Option[String]
}

private[enki] sealed trait ArgumentActionBase[T] extends StageAction[T] with ArgumentAction {
  override def defaultStringValue: Option[String] = defaultValue.map(_.toString)

  protected def fromParameter(extractor: PartialFunction[ParameterValue, T], parameterValue: ParameterValue): T = {
    extractor.lift(parameterValue) match {
      case Some(value) => value
      case None => throw new Exception(s"Invalid parameter type: required $dataType actual ${parameterValue.dataType}")
    }
  }

  protected def fromParameter(parameterValue: ParameterValue): T

  def defaultValue: Option[T]

  def fromParameterMap(parameters: Map[String, ParameterValue]): T = {
    (parameters.get(name), defaultValue) match {
      case (Some(parameterValue), _) => fromParameter(parameterValue)
      case (None, Some(value)) => value
      case (None, None) => throw new Exception(s"Parameter $name not found.")
    }
  }
}

final case class StringArgumentAction(name: String, description: String, defaultValue: Option[String])
  extends ArgumentActionBase[String] {
  override def dataType: DataType = StringType

  override def fromParameter(parameterValue: ParameterValue): String =
    fromParameter({ case StringValue(str) => str }, parameterValue)
}

final case class IntegerArgumentAction(name: String, description: String, defaultValue: Option[Int])
  extends ArgumentActionBase[Int] {
  override def dataType: DataType = IntegerType

  override def fromParameter(parameterValue: ParameterValue): Int =
    fromParameter({ case IntegerValue(int) => int }, parameterValue)
}
