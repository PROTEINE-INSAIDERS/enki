package enki

import cats.data._

trait Aliases {
  type EnkiMonad[A] = Reader[Environment, A]
}
