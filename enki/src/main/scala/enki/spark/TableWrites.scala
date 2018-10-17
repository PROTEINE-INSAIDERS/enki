package enki.spark

import cats._
import cats.data._
import freestyle.free._

class TableWrites[M: Monoid](f: WriteTableAction => M) extends FSHandler[SparkAlg.Op, Const[M, ?]] {
  override def apply[A](fa: SparkAlg.Op[A]): Const[M, A] = Const(fa match {
    case SparkAlg.WriteDataFrameOp(schemaName, tableName) => f(WriteDataFrameAction(schemaName, tableName))
    case SparkAlg.WriteDatasetOp(schemaName, tableName, encoder, strict) => f(WriteDatasetAction(schemaName, tableName, encoder, strict))
    case _ => Monoid.empty[M]
  })
}

