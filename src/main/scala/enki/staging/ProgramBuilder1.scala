package enki.staging

import cats.arrow.FunctionK
import cats.data.Const
import enki.staging
import cats._
import cats.arrow._
import cats.data._
import cats.free.FreeApplicative._
import cats.free._
import cats.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders._


trait ProgramBuilder1 {

  import cats._
  import cats.arrow._
  import cats.data._
  import cats.free.FreeApplicative._
  import cats.free._
  import cats.implicits._
  import org.apache.spark.sql._
  import org.apache.spark.sql.catalyst.encoders._

  import scala.reflect.runtime.universe.TypeTag

  sealed trait ProgramA[A]

  final case class StageOp[T](id: Symbol, stage: StagingFA[Dataset[T]]) extends ProgramA[StagingFA[Dataset[T]]]

  type Program[A] = FreeApplicative[ProgramA, A]

  def stage[T](id: Symbol, stage: StagingFA[Dataset[T]]): Program[StagingFA[Dataset[T]]] = lift[ProgramA, StagingFA[Dataset[T]]](StageOp[T](id, stage))

  def source[T: TypeTag]: StagingFA[Dataset[T]] =
    FreeApplicative.lift[StagingOp, Dataset[T]](SourceOp[T] { s => s.emptyDataset[T](ExpressionEncoder[T]()) })

  val s1 = stage[Row]('s1, source[Row])
  val s2 = stage[Row]('s2, source[Row])

  def f(d1: DataFrame, d2: DataFrame): DataFrame = {
    d1.join(d2, d1("_2") === d2("_1"))
  }

  def t(a: StagingFA[DataFrame], b: StagingFA[DataFrame]) = {
    (a, b) mapN f
  }


  val kk: Program[StagingFA[DataFrame]] = (s1, s1) mapN t

  type Log[A] = Const[List[String], A]

  val cc = new FunctionK[ProgramA, Id] {
    override def apply[A](fa: ProgramA[A]): Id[A] = fa match {
      case so: StageOp[t] => {
        println(so.id)
        // so.stage
        FreeApplicative.lift[StagingOp, Dataset[t]](enki.staging.StageOp[t](so.id))
      }
    }
  }


  /*

  def program: Program[StagingFA[DataFrame]] = {
    for {
      a <- stage[Row]('s1)
      b <- stage[Row]('s1)
    } yield  (a, b) mapN f
  }
  */
}

object test extends ProgramBuilder1 {
  def main(args: Array[String]): Unit = {
    val ddd = kk.foldMap(cc)
    println(ddd)

    type Log[A] = Const[List[String], A]

    val logCompiler = new FunctionK[staging.StagingOp, Log] {
      def apply[A](fa: staging.StagingOp[A]): Log[A] = fa match {
        case op => Const(List(s"$op"))
      }
    }

    val dd = ddd.foldMap(logCompiler)

    println(dd)
  }
}
