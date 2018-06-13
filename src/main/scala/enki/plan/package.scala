package enki

import cats.free.FreeApplicative

package object plan {
  type Plan[A] = FreeApplicative[PlanOp, A]
}
