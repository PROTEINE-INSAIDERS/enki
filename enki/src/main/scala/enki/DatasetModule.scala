package enki

import org.apache.spark.sql._

import scala.language.experimental.macros

trait DatasetModule extends OptionColumnRelation {
  this: DataFrameModule =>

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
  }

}
