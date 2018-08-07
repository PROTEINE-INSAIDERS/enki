package enki

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

trait DataFrameModule {
  private val diffStatusColName = "diff_status"
  private val addedStatus = "added"
  private val removedStatus = "removed"
  private val updatedStatus = "updated"
  private val oldValue = "old"
  private val newValue = "new"

  def diff(l: DataFrame,
           r: DataFrame,
           keyColumns: Seq[String],
           withEpsilon: Boolean): DataFrame = {

    val added = keyColumns.map(l(_).isNull).reduce(_ and _)
    val removed = keyColumns.map(r(_).isNull).reduce(_ and _)

    //TODO: найти лучший способ работать с таблицами, у которых не совпадают схемы.
    val columns = (l.schema.map(_.name).toSet intersect r.schema.map(_.name).toSet).toSeq

    def floatSafeEq[T](l: Column, r: Column, e: T): Column =
      (l.isNull && r.isNull) or not(abs(l - r) > e)

    def different(name: String) = not(l.schema(name).dataType match {
      case FloatType if withEpsilon => floatSafeEq(l(name), r(name), Float.MinPositiveValue)
      case DoubleType if withEpsilon => floatSafeEq(l(name), r(name), Double.MinPositiveValue)
      case _ => l(name) <=> r(name)
    })

    def colDiff(name: String): Column = struct(
      l(name).as(oldValue),
      r(name).as(newValue),
      when(added, addedStatus)
        .when(removed, removedStatus)
        .when(not(l(name) <=> r(name)), updatedStatus).as(diffStatusColName)
    )

    def rowStatus: Column =
      when(added, addedStatus)
        .when(removed, removedStatus)
        .when(not(columns.map(col => l(col) <=> r(col)).foldLeft(lit(true))(_ and _)), updatedStatus)

    l
      .join(r, keyColumns.map { c => l(c) === r(c) }.reduce(_ and _), "outer")
      .select(columns.map(c => colDiff(c).as(c)): _*)
      .withColumn(diffStatusColName, coalesce(columns.map(c => col(s"$c.$diffStatusColName")): _*))
    // .select(rowStatus.as(diffStatusColName) +: columns.map(c => colDiff(c).as(c)): _*)
  }

  /**
    * Convert diff frame to format similar to http://paulfitz.github.io/dataprotocols/tabular-diff-format/
    * TODO: механизм усранения коллизий по -> не реализован
    */
  def formatDiff(diff: DataFrame): DataFrame = {
    def formatActionColumn: Column =
      when(diff(diffStatusColName) === addedStatus, "+++")
        .when(diff(diffStatusColName) === removedStatus, "---")
        .when(diff(diffStatusColName) === updatedStatus, "->")

    def formatDiffColumn(colName: String): Column =
      when(diff(s"$colName.$diffStatusColName") === addedStatus, diff(s"$colName.$newValue"))
        .when(diff(s"$colName.$diffStatusColName") === removedStatus, diff(s"$colName.$oldValue"))
        .when(diff(s"$colName.$diffStatusColName") === updatedStatus, concat(diff(s"$colName.$oldValue"), lit("->"), diff(s"$colName.$newValue")))
        .otherwise(diff(s"$colName.$oldValue"))

    diff.select(
      formatActionColumn.as(diffStatusColName) +: diff.schema.filter(_.name != diffStatusColName).map(c => formatDiffColumn(c.name).as(c.name)): _*)
  }

  def fillna(dataFrame: DataFrame, value: Any): DataFrame = {
    dataFrame.select(dataFrame.columns.map(colName => coalesce(dataFrame(colName), lit(value)).as(colName)): _*)
  }

  def nonUnique(data: DataFrame, cols: Seq[Column]): DataFrame = {
    data.groupBy(cols: _*).count().where(col("count") > 1).select(cols: _*)
  }

  implicit class DataFrameExtensions(dataFrame: DataFrame) {
    def cast[T: TypeTag](strict: Boolean): Dataset[T] = {
      //TODO: move to module level?
      if (typeOf[T] == typeOf[Row]) {
        if (strict) {
          throw new Exception("Unable to restrict schema for generic type Row.")
        }
        dataFrame.asInstanceOf[Dataset[T]]
      } else {
        val expressionEncoder: ExpressionEncoder[T] = ExpressionEncoder[T]()
        if (strict) {
          val toolbox = currentMirror.mkToolBox()

          def instantiate(annotation: Annotation): Any = {
            toolbox.eval(toolbox.untypecheck(annotation.tree))
          }

          val bigDecimalReplacements = symbolOf[T].asClass.primaryConstructor.typeSignature.paramLists.head
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
              u.copy(dataType = bigDecimalReplacements(a.name))
          }

          val strictEncoder = expressionEncoder.copy(schema = schema, serializer = serializer, deserializer = deserializer)

          dataFrame.select(strictEncoder.schema.map(f => dataFrame(f.name)): _*).as[T](strictEncoder)
        } else {
          dataFrame.as[T](expressionEncoder)
        }
      }
    }

    /**
      * Diff current dataset against other.
      *
      * "Current" dataset considered "original" or "left" in diff operation.
      */
    def diff(other: DataFrame, keyColumns: Seq[String], withEpsilon: Boolean = false): DataFrame = {
      DataFrameModule.this.diff(dataFrame, other, keyColumns, withEpsilon)
    }

    def fillna(value: Any): DataFrame = {
      DataFrameModule.this.fillna(dataFrame, value)
    }

    def nonUniq(col: Column*): DataFrame = {
      DataFrameModule.this.nonUnique(dataFrame, col)
    }
  }

}
