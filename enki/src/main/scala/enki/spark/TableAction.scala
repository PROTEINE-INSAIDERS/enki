package enki
package spark

import org.apache.spark.sql._

//TODO: можно заменить на продукт из iota
//TODO: но на самом деле лучше унифицировать, т.к. эти штуки всё равно используются только для анализа.
trait TableAction {
  def schemaName: String

  def tableName: String
}

trait ReadTableAction extends TableAction


final case class ReadDataFrameAction(
                                      schemaName: String,
                                      tableName: String
                                    ) extends ReadTableAction

final case class ReadDatasetAction[T](
                                       schemaName: String,
                                       tableName: String,
                                       encoder: Encoder[T],
                                       strict: Boolean
                                     ) extends ReadTableAction

trait WriteTableAction extends TableAction

final case class WriteDataFrameAction(
                                       schemaName: String,
                                       tableName: String// ,
                                       // writerSettings: WriterSettings
                                     ) extends WriteTableAction

final case class WriteDatasetAction[T](
                                        schemaName: String,
                                        tableName: String,
                                        encoder: Encoder[T],
                                        strict: Boolean// ,
                                     //   writerSettings: WriterSettings
                                      ) extends WriteTableAction