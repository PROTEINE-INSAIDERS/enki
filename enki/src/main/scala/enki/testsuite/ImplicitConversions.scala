package enki.testsuite

import cats._

trait ImplicitConversions {
  implicit def enkiImplicitly[A, B](x: A)(implicit I: Inject[A, B]): B = I(x)

  implicit def enkiSomeInjectInstance[A]: Inject[A, Option[A]] = new Inject[A, Option[A]] {
    override def inj: A => Option[A] = Some(_)

    override def prj: Option[A] => Option[A] = identity
  }
}
