package enki
package stage

import org.apache.spark.sql._

//TODO: можно заменить на продукт из iota
trait TableAction {
  def schemaName: String

  def tableName: String

  override def toString: String = s"$schemaName.$tableName"
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
                                       // writerSettings: WriterSettings[Row]
                                     ) extends WriteTableAction

final case class WriteDatasetAction[T](
                                        schemaName: String,
                                        tableName: String,
                                        encoder: Encoder[T],
                                        strict: Boolean// ,
                                     //   writerSettings: WriterSettings[T]
                                      ) extends WriteTableAction