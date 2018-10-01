package enki.args

import enki._
import org.apache.spark.sql.types._

//TODO: Action - старый суфикс, когда финальные классы использовались в аппликативном функторе. Возможно следует переименовать.
//TODO: можно заменить на продукт из iota или вообще выкинуть и использовать только Args.
sealed trait ArgumentAction {
  def name: String

  def description: String

  def dataType: DataType

  private[enki] def defaultStringValue: Option[String]
}

private[enki] sealed trait ArgumentActionBase[T] extends ArgumentAction {
  override def defaultStringValue: Option[String] = defaultValue.map(_.toString)

  protected def fromParameter(extractor: PartialFunction[ParameterValue, T], parameterValue: ParameterValue): T = {
    extractor.lift(parameterValue) match {
      case Some(value) => value
      case None => throw new Exception(s"Invalid parameter type: required $dataType actual ${parameterValue.dataType}")
    }
  }

  protected def fromParameter(parameterValue: ParameterValue): T

  def defaultValue: Option[T]

  def fromParameterMap(parameters: Map[String, ParameterValue]): T = {
    (parameters.get(name), defaultValue) match {
      case (Some(parameterValue), _) => fromParameter(parameterValue)
      case (None, Some(value)) => value
      case (None, None) => throw new Exception(s"Parameter $name not found.")
    }
  }
}

final case class StringArgumentAction(name: String, description: String, defaultValue: Option[String])
  extends ArgumentActionBase[String] {
  override def dataType: DataType = StringType

  override def fromParameter(parameterValue: ParameterValue): String =
    fromParameter({ case StringValue(str) => str }, parameterValue)
}

final case class IntegerArgumentAction(name: String, description: String, defaultValue: Option[Int])
  extends ArgumentActionBase[Int] {
  override def dataType: DataType = IntegerType

  override def fromParameter(parameterValue: ParameterValue): Int =
    fromParameter({ case IntegerValue(int) => int }, parameterValue)
}

final case class BooleanArgumentAction(name: String, description: String, defaultValue: Option[Boolean])
  extends ArgumentActionBase[Boolean] {
  override def dataType: DataType = BooleanType

  override def fromParameter(parameterValue: ParameterValue): Boolean =
    fromParameter({ case BooleanValue(bool) => bool }, parameterValue)
}
