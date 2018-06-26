package enki

import cats.free.FreeApplicative

package object program {
  type Program[A] = FreeApplicative[program.Statement, A]
}
