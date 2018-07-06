package enki

import org.apache.spark.sql._

trait DatasetModule {
  this: DataFrameModule =>

  implicit class DatasetExtensions[T](dataset: Dataset[T]) {
    def fillna(value: Any)(implicit encoder: Encoder[T]): Dataset[T] = {
      DatasetModule.this.fillna(dataset.toDF(), value).as[T]
    }
  }
}
