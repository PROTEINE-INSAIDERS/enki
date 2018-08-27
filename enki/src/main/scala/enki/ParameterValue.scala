package enki

sealed trait ParameterValue {
  def typeName: String
}

final case class StringValue(value: String) extends ParameterValue {
  override def typeName: String = "String"
}

final case class IntegerValue(value: Int) extends ParameterValue {
  override def typeName: String = "Integer"
}