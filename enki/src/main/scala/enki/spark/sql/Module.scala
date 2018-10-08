package enki.spark.sql

import enki._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

import scala.language.experimental.macros

trait Module extends OptionTypeMapping {

  implicit class DatasetExtensions[T](val dataset: Dataset[T]) {
    def $[A, R](selector: T => A)
               (implicit relation: ColumnTypeMapping[A, R], encoder: Encoder[R]): TypedColumn[T, R] =
    macro DatasetMacros.column[T, A, R]

    def colName[R](selector: T => R): String = macro DatasetMacros.columnName[T, R]

    def dropCol[R](selector: T => R): DataFrame = macro DatasetMacros.drop[T, R]

    def typedCol[A, R](selector: T => A)
                      (implicit relation: ColumnTypeMapping[A, R], encoder: Encoder[R]): TypedColumn[T, R] =
    macro DatasetMacros.column[T, A, R]

    def fillna(value: Any)(implicit encoder: Encoder[T]): Dataset[T] = {
      dataset.toDF().fillna(value).as[T]
    }
  }

  /**
    * Null column with type information.
    */
  //TODO: проверить, можно ли вместо него использовать TypedLit.
  def typedNull[T: DataTypeMapping]: Column = {
    lit(null).cast(implicitly[DataTypeMapping[T]].dataType)
  }
}
