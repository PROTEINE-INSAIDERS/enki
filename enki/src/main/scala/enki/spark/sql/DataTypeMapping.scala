package enki.spark.sql

import java.sql.Timestamp

import org.apache.spark.sql.types._

trait DataTypeMapping[T] {
  def dataType: DataType
}

trait DataTypeMappings {

  implicit object BigDecimalMapping extends DataTypeMapping[BigDecimal] {
    override def dataType: DataType = DecimalType.SYSTEM_DEFAULT
  }

  implicit object BooleanMapping extends DataTypeMapping[Boolean] {
    override def dataType: DataType = BooleanType
  }

  implicit object IntMapping extends DataTypeMapping[Int] {
    override def dataType: DataType = IntegerType
  }

  implicit object LongMapping extends DataTypeMapping[Long] {
    override def dataType: DataType = LongType
  }

  implicit object StringMapping extends DataTypeMapping[String] {
    override def dataType: DataType = StringType
  }

  implicit object TimestampMapping extends DataTypeMapping[Timestamp] {
    override def dataType: DataType = TimestampType
  }
}