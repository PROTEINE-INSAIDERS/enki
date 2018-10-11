package enki
package spark

import cats._
import cats.data._
import cats.implicits._
import freestyle.free.FSHandler
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}
import org.apache.spark.sql.types.StructType

class TableReads[M: Monoid](f: ReadTableAction => M) extends FSHandler[SparkAlg.Op, Const[M, ?]]  {
  override def apply[A](fa: SparkAlg.Op[A]): Const[M, A] = Const(fa match {
    case SparkAlg.ReadDataFrameOp(schemaName, tableName) => f(ReadDataFrameAction(schemaName, tableName))
    case SparkAlg.ReadDatasetOp(schemaName, tableName, encoder, strict) => f(ReadDatasetAction(schemaName, tableName, encoder, strict))
    case _ => Monoid.empty[M]
  })
}

class TableWrites[M: Monoid](f: WriteTableAction => M) extends FSHandler[SparkAlg.Op, Const[M, ?]] {
  override def apply[A](fa: SparkAlg.Op[A]): Const[M, A] = Const(fa match {
    case SparkAlg.WriteDataFrameOp(schemaName, tableName) => f(WriteDataFrameAction(schemaName, tableName))
    case SparkAlg.WriteDatasetOp(schemaName, tableName, encoder, strict) => f(WriteDatasetAction(schemaName, tableName, encoder, strict))
    case _ => Monoid.empty[M]
  })
}

trait Analyzers {
  self: Enki =>

  def stageNonEmpty(stage: Stage[_]): Boolean = {
    stage.analyze(λ[StageOp ~> λ[α => Option[Unit]]] {
      case _ => Some(())
    }).nonEmpty
  }
}
