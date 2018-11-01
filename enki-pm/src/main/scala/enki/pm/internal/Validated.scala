package enki.pm.internal

import cats._
import cats.data.NonEmptyChain
import cats.implicits._

object Validated {
  object Valid {
    def apply[A](valid: A): Validated[A] = cats.data.Validated.valid(valid)

    def unapply[A](arg: Validated[A]): Option[A] = arg.toOption
  }

  object Invalid {
    def apply[A](message: String): Validated[A] = cats.data.Validated.invalidNec(message)

    def unapply[A](arg: Validated[A]): Option[ValidationErrorContainer[ValidationError]] = arg.toEither.left.toOption
  }

  def wrapError[F[_], E <: Throwable, A](fa: F[A])(implicit F: ApplicativeError[F, E]): F[Validated[A]] = {
    fa.map(Validated.Valid(_)).handleError(e => Validated.Invalid(e.getLocalizedMessage))
  }
}
