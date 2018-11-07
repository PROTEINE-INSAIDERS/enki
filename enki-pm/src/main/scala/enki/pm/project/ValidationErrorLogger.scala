package enki.pm.project

import cats._
import cats.effect._
import cats.implicits._
import cats.mtl._
import enki.pm.internal._
import io.chrisdavenport.log4cats._

case class ValidationErrorLogger[M[_] : LiftIO : Applicative](implicit logger: Logger[M]) extends DefaultFunctorTell[M, ValidationError] {
  override val functor: Functor[M] = implicitly[Functor[M]]

  override def tell(l: ValidationError): M[Unit] = l.traverse_(e => logger.warn(e))
}
