package enki.builder

import enki.devonly.Result

sealed trait ExecutionPlanBuilder[T]

final case class Fake[T]() extends ExecutionPlanBuilder[T]

final case class Source(id: Symbol) extends ExecutionPlanBuilder[Result]