package enki.arg

import java.sql.Timestamp

import scala.reflect.runtime.universe._

sealed trait ParameterValue {
  def dataType: Type
}

final case class BigIntValue(value: BigInt) extends ParameterValue {
  override def dataType: Type = typeOf[BigInt]
}

final case class BooleanValue(value: Boolean) extends ParameterValue {
  override def dataType: Type = typeOf[Boolean]
}

final case class IntegerValue(value: Int) extends ParameterValue {
  override def dataType: Type = typeOf[Int]
}

final case class StringValue(value: String) extends ParameterValue {
  override def dataType: Type = typeOf[String]
}

final case class TimestampValue(value: Timestamp) extends ParameterValue {
  override def dataType: Type = typeOf[Timestamp]
}