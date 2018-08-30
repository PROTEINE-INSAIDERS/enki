package enki.ds

import enki._
import org.apache.spark.sql._

import scala.language.experimental.macros

trait Extensions extends OptionTypeMapping {

  implicit class DatasetExtensions[T](val dataset: Dataset[T]) {
    def $[A, R](selector: T => A)
               (implicit relation: ColumnTypeMapping[A, R], encoder: Encoder[R]): TypedColumn[T, R] =
    macro DatasetMacros.column[T, A, R]

    def colName[R](selector: T => R): String =
    macro DatasetMacros.columnName[T, R]

    def typedCol[A, R](selector: T => A)
                      (implicit relation: ColumnTypeMapping[A, R], encoder: Encoder[R]): TypedColumn[T, R] =
    macro DatasetMacros.column[T, A, R]

    def fillna(value: Any)(implicit encoder: Encoder[T]): Dataset[T] = {
      dataset.toDF().fillna(value).as[T]
    }
  }

}
