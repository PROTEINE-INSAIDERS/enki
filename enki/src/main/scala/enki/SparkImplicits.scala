package enki

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders._

import scala.language.implicitConversions
import scala.reflect.runtime.universe._

//TODO: Consider encoder type-classes.
/**
  * Implicit definitions factored out from SparkSession.implicits to eliminate the need for importing them from
  * spark session each time.
  */
trait SparkImplicits extends SparkImplicits1 {

  // Columns creation

  //TODO: Why we need interpolation here?
  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): ColumnName = {
      new ColumnName(sc.s(args: _*))
    }
  }

  implicit def symbolToColumn(s: Symbol): ColumnName = new ColumnName(s.name.toString)

  // Primitive encoders

  implicit def intEncoder: Encoder[Int] = Encoders.scalaInt

  implicit def longEncoder: Encoder[Long] = Encoders.scalaLong

  implicit def doubleEncoder: Encoder[Double] = Encoders.scalaDouble

  implicit def floatEncoder: Encoder[Float] = Encoders.scalaFloat

  implicit def byteEncoder: Encoder[Byte] = Encoders.scalaByte

  implicit def shortEncoder: Encoder[Short] = Encoders.scalaShort

  implicit def booleanEncoder: Encoder[Boolean] = Encoders.scalaBoolean

  implicit def stringEncoder: Encoder[String] = Encoders.STRING

  implicit def javaDecimalEncoder: Encoder[java.math.BigDecimal] = Encoders.DECIMAL

  implicit def scalaDecimalEncoder: Encoder[scala.math.BigDecimal] = ExpressionEncoder()

  implicit def dateEncoder: Encoder[java.sql.Date] = Encoders.DATE

  implicit def timeStampEncoder: Encoder[java.sql.Timestamp] = Encoders.TIMESTAMP

  // Sequence and array encoders

  implicit def sequenceEncoder[T <: Seq[_] : TypeTag]: Encoder[T] = ExpressionEncoder()

  implicit def intArrayEncoder: Encoder[Array[Int]] = ExpressionEncoder()

  implicit def longArrayEncoder: Encoder[Array[Long]] = ExpressionEncoder()

  implicit def doubleArrayEncoder: Encoder[Array[Double]] = ExpressionEncoder()

  implicit def floatArrayEncoder: Encoder[Array[Float]] = ExpressionEncoder()

  implicit def byteArrayEncoder: Encoder[Array[Byte]] = Encoders.BINARY

  implicit def shortArrayEncoder: Encoder[Array[Short]] = ExpressionEncoder()

  implicit def booleanArrayEncoder: Encoder[Array[Boolean]] = ExpressionEncoder()

  implicit def stringArrayEncoder: Encoder[Array[String]] = ExpressionEncoder()

  implicit def productArrayEncoder[A <: Product : TypeTag]: Encoder[Array[A]] = ExpressionEncoder()
}

trait SparkImplicits1 {
  implicit def productEncoder[T <: Product : TypeTag]: Encoder[T] = Encoders.product[T]
}