package enki.dataset

import org.apache.spark.sql._

trait DataFrameSyntax {

  implicit class DataFrameExtensions(dataFrame: DataFrame) {
    def fillna(value: Any): DataFrame = {
      Functions.fillna(dataFrame, value)
    }
  }

}
