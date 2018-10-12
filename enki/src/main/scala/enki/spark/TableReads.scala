package enki.spark

import cats._
import cats.data._
import freestyle.free._

class TableReads[M: Monoid](f: ReadTableAction => M) extends FSHandler[SparkAlg.Op, Const[M, ?]] {
  override def apply[A](fa: SparkAlg.Op[A]): Const[M, A] = Const(fa match {
    case SparkAlg.ReadDataFrameOp(schemaName, tableName) => f(ReadDataFrameAction(schemaName, tableName))
    case SparkAlg.ReadDatasetOp(schemaName, tableName, encoder, strict) => f(ReadDatasetAction(schemaName, tableName, encoder, strict))
    case _ => Monoid.empty[M]
  })
}
