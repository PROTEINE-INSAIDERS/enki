package enki.staging


import cats.{Id, ~>}
import org.apache.spark.sql.{Dataset, SparkSession}
import shapeless._

// источники и стейджи надо именовать, при этом должна быть возможность как автоматического именования,
// так и с помощью генератора.
// именование потребуется в процессе добавления их в граф.
sealed trait StagingOp[A]

trait Source[A] {
  type Row

  def get(session: SparkSession): Dataset[Row]
}

final case class SourceOp[S, T](source: Source[S]) extends StagingOp[Dataset[T]]

final case class StageOp[T](program: StagingF[Dataset[T]]) extends StagingOp[Dataset[T]]

trait Syntax {

}

object Test {
  // 1. затягивать source через witness - идея интересная, но её надо реализовать поверх DSL-а, чтобы не усложнять его.
  // минимально souce - это функция из Session в Dataset.
  def source(source: WitnessWith[Source]): StagingOp[Dataset[source.instance.Row]] =
    SourceOp[source.T, source.instance.Row](source.instance)

  implicit val r1 = new Source[Witness.`'source1`.T] {
    type Row = (Int, Int)

    override def get(session: SparkSession): Dataset[(Int, Int)] = ???
  }

  implicit val r2 = new Source[Witness.`'source2`.T] {
    type Row = String

    override def get(session: SparkSession): Dataset[String] = ???
  }

  val aaa = source('source1)

  def bbb(k: StagingOp[Dataset[(Int, Int)]]) = {}

  bbb(aaa)

  def evaluator(implicit session: SparkSession): StagingOp ~> Id = λ[StagingOp ~> Id] {
    case so: SourceOp[s, t] =>
      so.source.get(session) // здесь нет доказательства, что source.Row === Dataset[T]
    case StageOp(p) => ???
  }


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
