package enki.dataset

import org.apache.spark.sql._

trait DataFrameSyntax {

  implicit class DataFrameExtensions(dataFrame: DataFrame) {
    def fillna(value: Any): DataFrame = {
      functions.fillna(dataFrame, value)
    }
  }

}
