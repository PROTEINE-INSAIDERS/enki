package enki
package compiler

import cats._
import enki.program._
import org.apache.spark.sql._

trait Compiler {
  def sourceMapper(f: Read ~> Read): Statement ~> Statement = λ[Statement ~> Statement] {
    case sourceOp: Read[t] => f(sourceOp)
    case other => other // это приведет к пропуску stage
  }

  def evaluator(implicit session: SparkSession): Statement ~> Id = λ[Statement ~> Id] {
    case readSt: Read[t] =>
      readSt.source.read[t](readSt.name, session)(readSt.typeTag)
    case Session => session
    case Stage(name, _) =>
      //TODO: решить, как вычислять stage - простой select, либо построение заново.
      ???
    case writeSt: Write[t] => (data: Dataset[t]) =>
      writeSt.writer.write[t](writeSt.name, data, session)(writeSt.typeTag)
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
