package enki.staging

import cats._
import cats.implicits._
import cats.Monad
import cats.data.{RWST, StateT}
import cats.effect.Effect

case class MyState()

case class PT[F[_], A](protected val unPT: StateT[F,  MyState, A])

object PT {
  implicit def ptMonad[F[_]](implicit F: Monad[F]): Monad[PT[F, ?]] = new Monad[PT[F, ?]] {
    override def pure[A](x: A): PT[F, A] = PT[F, A](StateT.pure[F, MyState, A](x))

    override def flatMap[A, B](fa: PT[F, A])(f: A => PT[F, B]): PT[F, B] = PT[F, B](fa.unPT.flatMap(s => f(s).unPT))

    override def tailRecM[A, B](a: A)(f: A => PT[F, Either[A, B]]): PT[F, B] = PT[F, B](a.tailRecM(k => f(k).unPT))
  }

  def test1[F[_]](implicit F: Monad[F]) = PT(10.pure[StateT[F, MyState, ?]])

  val y = for (a <- test1[Eval]) yield  a

  val d = y.unPT.run(MyState())

  def main(args: Array[String]) = {
  //  val a : Effect = ???
    println(d.value)
  }
}
