package enki
package interpreter

import cats._
import cats.data.Tuple2K
import enki.program._
import org.apache.spark.sql._

trait Interpreter {
  def sourceMapper(f: SourceSt ~> SourceSt): Statement ~> Statement = λ[Statement ~> Statement] {
    case sourceOp: SourceSt[t] => f(sourceOp)
    case other => other
  }

  def evaluator(session: SparkSession): Statement ~> Id = λ[Statement ~> Id] {
    case sourceOp: SourceSt[t] =>
      sourceOp.source.read[t](sourceOp.name, session)(sourceOp.typeTag)
    case SessionSt => session
    case StageSt(name, _) =>
      //TODO: решить, как вычислять stage - простой select, либо построение заново.
      ???
  }

  def eval[T](plan: Program[T])(implicit session: SparkSession): T = {
    plan.foldMap(evaluator(session))
  }

  //TODO: unstage - получить свободную структуру, где все операции будут отмечены стейджем, к которому они относятся.
  def aaa[T](plan: Program[T]) = {
    //TODO: 

    // val bbb = plan.foldMap(λ[Statement ~> (stage, Statement[?])] {
    //  case _ => (10, ???)
    //})


    ???
  }
}
