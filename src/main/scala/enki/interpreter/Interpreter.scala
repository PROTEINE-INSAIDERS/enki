package enki
package interpreter

import cats._
import cats.data._
import cats.implicits._
import enki.plan._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders._

import scala.reflect.runtime.universe.{TypeTag, typeOf}

trait Interpreter {
  //TODO: это соберет источники только для текущего Stage, поскольку не интерпретирует
  //TODO: реально создавать источники не нужно, но для отладки может быть полезно преобразование,
  // которое заменяет источники на пустые датасеты.
  private def emptySourcesCreator(session: SparkSession): PlanOp ~> Const[Unit, ?] = λ[PlanOp ~> Const[Unit, ?]] {
    case sourceOp: SourceOp[t] =>
      // session.emptyDataset[t](expressionEncoder[t](source.typeTag)).write.saveAsTable(source.qualifiedTableName)
      Const(())
    case _ =>
      Const(())
  }

  def sourceMapper(f: SourceOp ~> SourceOp): PlanOp ~> PlanOp = λ[PlanOp ~> PlanOp] {
    case sourceOp: SourceOp[t] => f(sourceOp)
    case other => other
  }

  def evaluator(session: SparkSession): PlanOp ~> Id = λ[PlanOp ~> Id] {
    case sourceOp: SourceOp[t] =>
      sourceOp.source.read[t](sourceOp.name, session)(sourceOp.typeTag)
    case SessionOp => session
    case StageOp(name, plan) =>
      //TODO: решить, как вычислять stage - простой select, либо построение заново.
      ???
  }

  def eval[T](plan: Plan[T])(implicit session: SparkSession): T = {
    plan.foldMap(evaluator(session))
  }
}
