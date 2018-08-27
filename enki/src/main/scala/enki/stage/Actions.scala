package enki.stage

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

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

trait WriteTableAction extends TableAction

final case class WriteDataFrameAction(
                                       schemaName: String,
                                       tableName: String,
                                       saveMode: Option[SaveMode]
                                     ) extends StageAction[DataFrame => Unit] with WriteTableAction

final case class WriteDatasetAction[T](
                                        schemaName: String,
                                        tableName: String,
                                        encoder: Encoder[T],
                                        strict: Boolean,
                                        saveMode: Option[SaveMode]
                                      ) extends StageAction[Dataset[T] => Unit] with WriteTableAction

sealed trait ArgumentType
object StringArgument extends ArgumentType
object IntegerArgument extends ArgumentType

trait ArgumentAction {
  def name: String

  //TODO: remove?
  def argumentType: ArgumentType

  def typeName: String
}

final case class StringArgument(name: String) extends StageAction[String] with ArgumentAction {
  override def typeName: String = "String"

  override def argumentType: ArgumentType = StringArgument
}

final case class IntegerArgument(name: String) extends StageAction[Int] with ArgumentAction {
  override def typeName: String = "Integer"

  override def argumentType: ArgumentType = IntegerArgument
}