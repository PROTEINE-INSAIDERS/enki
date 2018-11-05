package enki.pm.internal

import cats._
import cats.implicits._
import qq.droste._
import qq.droste.data._
import qq.droste.data.prelude._

object Annotating {
  /**
    * Attribute of the root node
    */
  def attr[F[_], A](fa: Attr[F, A]): A = Attr.un[F, A](fa)._1

  /**
    * Strip attribute from root
    */
  def strip[F[_], A](fa: Attr[F, A]): F[Attr[F, A]] = Attr.un[F, A](fa)._2

  /**
    * strip all attributes
    */
  def stripAll[F[_]: Functor, A](fa: Attr[F, A]): Fix[F] = {
    val alg = Algebra[AttrF[F, A, ?], Fix[F]] { case AttrF(_, x) => Fix(x) }
    scheme.cata(alg).apply(fa)
  }

  /**
    * annotation constructor
    */
  def ann[F[_], A](f: F[Attr[F, A]], a: A): Attr[F, A] = {
    Attr(a, f)
  }

  /**
    * annotation deconstructor
    */
  def unAnn[F[_], A](ann: Attr[F, A]): (F[Attr[F, A]], A) = {
    Attr.un(ann).swap
  }

  /**
    * Synthesized attributes are created in a bottom-up traversal using a catamorphism.
    */
  def synthesize[F[_]: Functor, A](f: F[A] => A, fx: Fix[F]): Attr[F, A] = {
    // alg = ann . (id &&& f . fmap attr)
    val alg = Algebra[F, Attr[F, A]] { fa => ann[F, A](fa, f(fa fmap attr)) }
    scheme.cata(alg).apply(fx)
  }
}
