package enki

import cats._
import freestyle.free.FreeS.Par
import freestyle.free._
import freestyle.free.implicits._

@free trait Alg1 {
  def m1(str: String): FS[String]
}

@free trait Alg2 {
  def m2(): FS[String => Unit]
}

@module trait Combined[FF$311[_]] extends _root_.freestyle.free.internal.EffectLike[FF$311] {
  val a1: Alg1[FF$311]
  val a2: Alg2[FF$311]
}

object ComposableAlg {
  implicit val h1: Alg1.Handler[Id] = new Alg1.Handler[Id] {
    override protected[this] def m1(str: String): Id[String] = str
  }

  implicit val h2: Alg2.Handler[Id] = new Alg2.Handler[Id] {
    override protected[this] def m2(): Id[String => Unit] = println(_)
  }

  def p1[F[_]](implicit c: Combined[F]): Par[F, Unit] = {
    import c.a1._
    import c.a2._

    m1("test").ap(m2())
  }

  def main(args: Array[String]): Unit = {
    val p = p1[Combined.Op].interpret[Id]
  }
}
