import cats.free.FreeApplicative
import enki.plan.PlanOp

package object enki {
  type Plan[A] = FreeApplicative[PlanOp, A]
}
