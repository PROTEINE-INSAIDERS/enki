package enki

import cats.free.FreeApplicative

package object staging {
  type StagingF[A] = FreeApplicative[staging.StagingOp, A]
}
