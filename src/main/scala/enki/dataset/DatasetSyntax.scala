package enki.dataset

import org.apache.spark.sql._

trait DatasetSyntax {

  implicit class DatasetExtensions[T](dataset: Dataset[T]) {
    def fillna(value: Any)(implicit encoder: Encoder[T]): Dataset[T] = {
      Functions.fillna(dataset.toDF(), value).as[T]
    }
  }

}
