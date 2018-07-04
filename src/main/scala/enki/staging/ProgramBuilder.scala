package enki.staging

import cats.Eval

trait ProgramBuilder {

  import cats._
  import cats.data.RWST._
  import cats.data._
  import cats.free.FreeApplicative
  import cats.implicits._
  import org.apache.spark.sql._
  import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

  import scala.reflect.runtime.universe.TypeTag

  type ProgramBuilderE = Unit
  type ProgramBuilderL = Unit
  type ProgramBuilderS = Map[Symbol, StagingFA[_]]

  //TODO: решить проблему с выведением типов. Возможно следует использовать паттрерн free applicative
  final case class ProgramBuilderT[F[_], A](private[ProgramBuilder] val unProgramBuilder: RWST[F, ProgramBuilderE, ProgramBuilderL, ProgramBuilderS, A])

  def runProgramBuilderT[F[_] : Monad, A](x: ProgramBuilderT[F, A])
                                         (e: ProgramBuilderE, s: ProgramBuilderS): F[(ProgramBuilderL, ProgramBuilderS, A)] =
    x.unProgramBuilder.run(e, s)

  def runProgramBuilder[A](x: ProgramBuilderT[Eval, A])
                          (e: ProgramBuilderE, s: ProgramBuilderS): (ProgramBuilderL, ProgramBuilderS, A) =
    runProgramBuilderT(x)(e, s).value

  type ProgramBuilder[A] = ProgramBuilderT[Eval, A]

  final class ProgramBuilderMonad[F[_] : Monad] extends Monad[ProgramBuilderT[F, ?]] {
    override def pure[A](x: A): ProgramBuilderT[F, A] = ProgramBuilder.this.pure(x)

    override def flatMap[A, B](fa: ProgramBuilderT[F, A])(f: A => ProgramBuilderT[F, B]): ProgramBuilderT[F, B] = ProgramBuilder.this.flatMap(fa)(f)

    override def tailRecM[A, B](a: A)(f: A => ProgramBuilderT[F, Either[A, B]]): ProgramBuilderT[F, B] = ProgramBuilder.this.tailRecM(a)(f)
  }

  implicit def programBuilderMonad[F[_] : Monad]: ProgramBuilderMonad[F] = new ProgramBuilderMonad[F]()

  // Common functions

  def pure[F[_] : Applicative, A](x: A): ProgramBuilderT[F, A] =
    ProgramBuilderT(RWST.pure[F, ProgramBuilderE, ProgramBuilderL, ProgramBuilderS, A](x))

  def flatMap[F[_] : FlatMap, A, B](fa: ProgramBuilderT[F, A])(f: A => ProgramBuilderT[F, B]): ProgramBuilderT[F, B] =
    ProgramBuilderT(fa.unProgramBuilder.flatMap(a => f(a).unProgramBuilder))

  def tailRecM[F[_] : Monad, A, B](a: A)(f: A => ProgramBuilderT[F, Either[A, B]]): ProgramBuilderT[F, B] =
    ProgramBuilderT(a.tailRecM(x => f(x).unProgramBuilder))

  // Program functions

  //TODO: provide sensible impelemtation
  def source[T: TypeTag]: StagingFA[Dataset[T]] =
    FreeApplicative.lift[StagingOp, Dataset[T]](SourceOp[T] { s => s.emptyDataset[T](ExpressionEncoder[T]()) })

  def stageRef[T](stage: Symbol): StagingFA[Dataset[T]] =
    FreeApplicative.lift[StagingOp, Dataset[T]](StageOp[T](stage))

  def stage[F[_] : Monad, T](id: Symbol, program: StagingFA[Dataset[T]]): ProgramBuilderT[F, StagingFA[Dataset[T]]] =
    ProgramBuilderT[F, StagingFA[Dataset[T]]](
      for {
        _ <- modify[F, ProgramBuilderE, ProgramBuilderL, ProgramBuilderS] { s =>
          if (s.contains(id)) {
            throw new Exception(s"Stage $id already registered.")
          } else {
            s + (id -> program)
          }
        }
      } yield {
        stageRef(id)
      }
    )

  //// -----------------------------------------

  def s1 = stage[Eval, (Int, Int)]('s1, source[(Int, Int)])
  val s2 = stage[Eval, (Int, String)]('s2, source[(Int, String)])

  def f(d1: Dataset[(Int, Int)], d2: Dataset[(Int, String)]): DataFrame = {
    d1.join(d2, d1("_2") === d2("_1"))
  }

  val p = for {
    a <- s1
    _ <- s1
    b <- s2
  } yield { (a, b) mapN f }

/*
  def test[F[_] : Monad]: ProgramBuilderT[F, StagingFA[DataFrame]] = for {
    a <- stage[F, Row]('stag1, p)
  } yield a
*/

}

object aaa extends ProgramBuilder {
  def main(args: Array[String]) = {
    val ddd = runProgramBuilderT(p)(Unit, Map()).value
   println(ddd)
  }
}