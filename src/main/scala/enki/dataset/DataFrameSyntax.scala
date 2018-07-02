package enki
package dataset

import org.apache.spark.sql._

trait DataFrameSyntax {

  implicit class DataFrameExtensions(dataFrame: DataFrame) {
    def fillna(value: Any): DataFrame = {
      functions.fillna(dataFrame, value)
    }


    /**
      * Diff current dataset against other.
      *
      * "Current" dataset considered "original" or "left" in diff operation.
      */
    def diff(other: DataFrame, keyColumns: Seq[String]): DataFrame = {
      functions.diff(dataFrame, other, keyColumns)
    }
  }
}
