package enki.dataset

import enki.DataFrameModule
import org.apache.spark.sql._

trait DatasetSyntax {
  this: DataFrameModule =>

  implicit class DatasetExtensions[T](dataset: Dataset[T]) {
    def fillna(value: Any)(implicit encoder: Encoder[T]): Dataset[T] = {
      functions.fillna(dataset.toDF(), value).as[T]
    }
  }

}
