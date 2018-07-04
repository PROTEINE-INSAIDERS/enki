package enki

import cats.free.FreeApplicative

package object staging {
  type StagingFA[A] = FreeApplicative[staging.StagingOp, A]
}
