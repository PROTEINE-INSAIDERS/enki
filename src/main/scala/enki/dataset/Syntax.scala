package enki.dataset

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

//TODO: сделать синтаксис для Dataset и DataFrame и приоритизацию имплицитов.
trait Syntax {
  //TODO: версия для DataFrame не будет использовать encoder.
  implicit class DatasetExtensions[T](dataset: Dataset[T]) {
    //TODO: добавить обработку NaN-ов как в pandas.
    def fillna(value: Any)(implicit encoder: Encoder[T]): Dataset[T] = {
      dataset.select(dataset.columns.map(colName => coalesce(dataset(colName), lit(value)).as(colName)): _*).as[T]
    }
  }
}
