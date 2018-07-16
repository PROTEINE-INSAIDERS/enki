package enki

import java.sql.Timestamp

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait SqlModule {

  trait TypeMapping[T] {
    def dataType: DataType
  }

  implicit object BigDecimalMapping extends TypeMapping[BigDecimal] {
    override def dataType: DataType = DecimalType.SYSTEM_DEFAULT
  }

  implicit object BooleanMapping extends TypeMapping[Boolean] {
    override def dataType: DataType = BooleanType
  }

  implicit object IntMapping extends TypeMapping[Int] {
    override def dataType: DataType = IntegerType
  }

  implicit object LongMapping extends TypeMapping[Long] {
    override def dataType: DataType = LongType
  }

  implicit object StringMapping extends TypeMapping[String] {
    override def dataType: DataType = StringType
  }

  implicit object TimestampMapping extends TypeMapping[Timestamp] {
    override def dataType: DataType = TimestampType
  }

  /**
    * Null column with type information.
    */
  //TODO: проверить, можно ли вместо него использовать TypedLit.
  def typedNull[T: TypeMapping]: Column = {
    lit(null).cast(implicitly[TypeMapping[T]].dataType)
  }
}
