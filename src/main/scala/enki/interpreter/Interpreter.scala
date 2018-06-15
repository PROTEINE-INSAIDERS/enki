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
  private def expressionEncoder[T](tag: TypeTag[T]): Encoder[T] = ExpressionEncoder[T]()(tag)

  private def decode[T: TypeTag](dataFrame: DataFrame): Dataset[T] = {
    if (typeOf[T] == typeOf[Row]) {
      dataFrame.asInstanceOf[Dataset[T]]
    } else {
      implicit val encoder: ExpressionEncoder[T] = ExpressionEncoder[T]()
      dataFrame.as[T]
    }
  }

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

  def createEmptySources(plan: Plan[_])(implicit session: SparkSession): Unit = {
    plan.foldMap(emptySourcesCreator(session))

    //TODO: использовать plan.analyze() для отображения плана в моноид.
  }


  private def evaluator(session: SparkSession): PlanOp ~> Id = λ[PlanOp ~> Id] {
    case sourceOp: SourceOp[t] =>
      sourceOp.source.decode[t](sourceOp.source.read(sourceOp.name, session))(sourceOp.typeTag)
    case SessionOp => session
    case StageOp(name, plan) =>
      //TODO: решить, как вычислять stage - простой select, либо построение заново.
      ???
  }

  def eval[T](plan: Plan[T])(implicit session: SparkSession): T = {
    plan.foldMap(evaluator(session))
  }
}

