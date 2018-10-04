package enki

import org.apache.spark.sql.types._

sealed trait ParameterValue {
  def dataType: DataType
}

final case class StringValue(value: String) extends ParameterValue {
  override def dataType: DataType = StringType
}

final case class IntegerValue(value: Int) extends ParameterValue {
  override def dataType: DataType = IntegerType
}

final case class BooleanValue(value: Boolean) extends ParameterValue {
  override def dataType: DataType = BooleanType
}