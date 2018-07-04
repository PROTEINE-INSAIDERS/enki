package enki.staging


import cats.{Id, ~>}
import org.apache.spark.sql.{Dataset, SparkSession}
import shapeless._

// источники и стейджи надо именовать, при этом должна быть возможность как автоматического именования,
// так и с помощью генератора.
// именование потребуется в процессе добавления их в граф.
sealed trait StagingOp[A]

final case class SourceOp[T](f: SparkSession => Dataset[T]) extends StagingOp[Dataset[T]]

final case class StageOp[T](stage: Symbol) extends StagingOp[Dataset[T]]

trait Syntax {

}

object Test {

/*
  def evaluator(implicit session: SparkSession): StagingOp ~> Id = λ[StagingOp ~> Id] {
    case so: SourceOp[s, t] =>
      so.source.get(session) // здесь нет доказательства, что source.Row === Dataset[T]
    case StageOp(p) => ???
  }
*/

  /*
  val (wTrue, wFalse) = (Witness(true), Witness(false))

  type True = wTrue.T
  type False = wFalse.T

  trait Select[B] { type Out }

  implicit val selInt = new Select[True] { type Out = Int }
  implicit val selString = new Select[False] { type Out = String }

  def select(b: WitnessWith[Select])(t: b.instance.Out) = t

  select(true)(23)
*/

  import syntax.std.tuple._

  val hlist = 23 :: "foo" :: true :: HNil

  val aa = hlist.apply(2)
}
