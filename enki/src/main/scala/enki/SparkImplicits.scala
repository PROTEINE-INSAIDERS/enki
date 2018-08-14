package enki

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.types._

import scala.reflect.runtime._
import scala.reflect.runtime.universe._
import scala.tools.reflect._

trait SparkImplicits extends SparkImplicits1 {
  def encoderStyle: EncoderStyle = EncoderStyle.Spark

  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): ColumnName = {
      new ColumnName(sc.s(args: _*))
    }
  }

  implicit def symbolToColumn(s: Symbol): ColumnName = new ColumnName(s.name.toString)

  // Enki-styleEncoder

  private def adjustUsingMetadata[R: TypeTag](expressionEncoder: ExpressionEncoder[R]): Encoder[R] = {
    val toolbox = currentMirror.mkToolBox()

    def instantiate(annotation: Annotation): Any = {
      toolbox.eval(toolbox.untypecheck(annotation.tree))
    }

    val bigDecimalReplacements = symbolOf[R].asClass.primaryConstructor.typeSignature.paramLists.head
      .map { symbol =>
        symbol.name.toString -> symbol.annotations.filter(_.tree.tpe <:< typeOf[decimalPrecision]).map(instantiate(_).asInstanceOf[decimalPrecision])
      }
      .flatMap {
        case (name, a :: Nil) => Some(name -> (DecimalType(a.precision, a.scale), a.allowTruncate))
        case (_, Nil) => None
        case (name, _) => throw new Exception(s"Multiple ${typeOf[decimalPrecision]} annotations applied to $name.")
      }.toMap

    val schema = StructType(expressionEncoder.schema.map {
      case f: StructField => bigDecimalReplacements.get(f.name) match {
        case Some((dataType, _)) if f.dataType.isInstanceOf[DecimalType] => f.copy(dataType = dataType)
        case Some(_) => throw new Exception(s"${typeOf[decimalPrecision]} annotation applied to non-decimal field ${f.name}.")
        case None => f
      }
      case other => other
    })

    val serializer = expressionEncoder.serializer.map {
      case a: Alias => bigDecimalReplacements.get(a.name) match {
        case Some((dataType, _)) => a.transform { case s: StaticInvoke => s.copy(dataType = dataType) }
        case None => a
      }
      case other => other
    }

    val deserializer = expressionEncoder.deserializer.transform {
      case u@UpCast(a@UnresolvedAttribute(_), _, _) => bigDecimalReplacements.get(a.name) match {
        case Some((dataType, allowTruncate)) => if (allowTruncate) {
          Cast(u.child, dataType, None)
        } else {
          u.copy(dataType = dataType)
        }
        case None => u
      }
    }

    expressionEncoder.copy(schema = schema, serializer = serializer, deserializer = deserializer)
  }

  private [enki] def selectEncoder[T: TypeTag](sparkEncoder: => Encoder[T]): Encoder[T] = encoderStyle match {
    case EncoderStyle.Spark => sparkEncoder
    case EncoderStyle.Enki => adjustUsingMetadata(ExpressionEncoder())
  }

  // Primitive encoders

  implicit def intEncoder: Encoder[Int] = selectEncoder(Encoders.scalaInt)

  implicit def longEncoder: Encoder[Long] = selectEncoder(Encoders.scalaLong)

  implicit def doubleEncoder: Encoder[Double] = selectEncoder(Encoders.scalaDouble)

  implicit def floatEncoder: Encoder[Float] = selectEncoder(Encoders.scalaFloat)

  implicit def byteEncoder: Encoder[Byte] = selectEncoder(Encoders.scalaByte)

  implicit def shortEncoder: Encoder[Short] = selectEncoder(Encoders.scalaShort)

  implicit def booleanEncoder: Encoder[Boolean] = selectEncoder(Encoders.scalaBoolean)

  implicit def stringEncoder: Encoder[String] = selectEncoder(Encoders.STRING)

  implicit def javaDecimalEncoder: Encoder[java.math.BigDecimal] = selectEncoder(Encoders.DECIMAL)

  implicit def scalaDecimalEncoder: Encoder[scala.math.BigDecimal] = selectEncoder(ExpressionEncoder())

  implicit def dateEncoder: Encoder[java.sql.Date] = selectEncoder(Encoders.DATE)

  implicit def timeStampEncoder: Encoder[java.sql.Timestamp] = selectEncoder(Encoders.TIMESTAMP)

  // Sequence and array encoders

  implicit def sequenceEncoder[T <: Seq[_] : TypeTag]: Encoder[T] = selectEncoder(ExpressionEncoder())

  implicit def intArrayEncoder: Encoder[Array[Int]] = selectEncoder(ExpressionEncoder())

  implicit def longArrayEncoder: Encoder[Array[Long]] = selectEncoder(ExpressionEncoder())

  implicit def doubleArrayEncoder: Encoder[Array[Double]] = selectEncoder(ExpressionEncoder())

  implicit def floatArrayEncoder: Encoder[Array[Float]] = selectEncoder(ExpressionEncoder())

  implicit def byteArrayEncoder: Encoder[Array[Byte]] = selectEncoder(Encoders.BINARY)

  implicit def shortArrayEncoder: Encoder[Array[Short]] = selectEncoder(ExpressionEncoder())

  implicit def booleanArrayEncoder: Encoder[Array[Boolean]] = selectEncoder(ExpressionEncoder())

  implicit def stringArrayEncoder: Encoder[Array[String]] = selectEncoder(ExpressionEncoder())

  implicit def productArrayEncoder[A <: Product : TypeTag]: Encoder[Array[A]] = selectEncoder(ExpressionEncoder())
}

trait SparkImplicits1 {
  self: SparkImplicits =>
  implicit def productEncoder[T <: Product : TypeTag]: Encoder[T] = self.selectEncoder(Encoders.product[T])
}