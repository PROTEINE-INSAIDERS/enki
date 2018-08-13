package enki

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.types._

import scala.language.experimental.macros
import scala.reflect.runtime._
import scala.reflect.runtime.universe._
import scala.tools.reflect._


trait DatasetModule extends OptionColumnRelation {
  this: DataFrameModule =>

  private[enki] def adjustUsingMetadata[R: TypeTag](expressionEncoder: ExpressionEncoder[R], allowTruncation: Boolean): Encoder[R] = {
    val toolbox = currentMirror.mkToolBox()

    def instantiate(annotation: Annotation): Any = {
      toolbox.eval(toolbox.untypecheck(annotation.tree))
    }

    val bigDecimalReplacements = symbolOf[R].asClass.primaryConstructor.typeSignature.paramLists.head
      .map { symbol =>
        symbol.name.toString -> symbol.annotations.filter(_.tree.tpe <:< typeOf[decimalPrecision]).map(instantiate(_).asInstanceOf[decimalPrecision])
      }
      .flatMap {
        case (name, a :: Nil) => Some((name, DecimalType(a.precision, a.scale)))
        case (_, Nil) => None
        case (name, _) => throw new Exception(s"Multiple ${typeOf[decimalPrecision]} annotations applied to $name.")
      }.toMap

    val schema = StructType(expressionEncoder.schema.map {
      case f: StructField if bigDecimalReplacements.contains(f.name) => f.copy(dataType = bigDecimalReplacements(f.name))
      case other => other
    })

    val serializer = expressionEncoder.serializer.map {
      case a: Alias if bigDecimalReplacements.contains(a.name) =>
        a.transform {
          case s: StaticInvoke =>
            s.copy(dataType = bigDecimalReplacements(a.name))
        }
      case other => other
    }

    val deserializer = expressionEncoder.deserializer.transform {
      case u@UpCast(a@UnresolvedAttribute(_), _, _) if bigDecimalReplacements.contains(a.name) =>
        if (allowTruncation) {
          Cast(u.child, bigDecimalReplacements(a.name), None)
        } else {
          u.copy(dataType = bigDecimalReplacements(a.name))
        }
    }

    expressionEncoder.copy(schema = schema, serializer = serializer, deserializer = deserializer)
  }

  implicit class DatasetExtensions[T](val dataset: Dataset[T]) {
    def $[A, R](selector: T => A)
               (implicit relation: ColumnTypeRelation[A, R], encoder: Encoder[R]): TypedColumn[T, R] =
    macro DatasetMacros.column[T, A, R]

    def columnName[R](selector: T => R): String =
    macro DatasetMacros.columnName[T, R]

    def typedCol[A, R](selector: T => A)
                      (implicit relation: ColumnTypeRelation[A, R], encoder: Encoder[R]): TypedColumn[T, R] =
    macro DatasetMacros.column[T, A, R]

    def fillna(value: Any)(implicit encoder: Encoder[T]): Dataset[T] = {
      DatasetModule.this.fillna(dataset.toDF(), value).as[T]
    }

    def cast[R: TypeTag](strict: Boolean, allowTruncate: Boolean): Dataset[R] = {
      //TODO: move to module level?
      if (typeOf[R] == typeOf[Row]) {
        if (strict) {
          throw new Exception("Unable to restrict schema for generic type Row.")
        }
        dataset.asInstanceOf[Dataset[R]]
      } else {
        val expressionEncoder: ExpressionEncoder[R] = ExpressionEncoder[R]()
        if (strict) {
          val strictEncoder = adjustUsingMetadata(expressionEncoder, allowTruncate)
          dataset.select(strictEncoder.schema.map(f => dataset(f.name)): _*).as[R](strictEncoder)
        } else {
          dataset.as[R](expressionEncoder)
        }
      }
    }
  }

}
