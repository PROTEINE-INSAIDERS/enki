package enki.dataset

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

//TODO: сделать trait?
private[enki] object functions {
  //TODO: добавить обработку NaN-ов как в pandas.
  def fillna(dataFrame: DataFrame, value: Any): DataFrame = {
    dataFrame.select(dataFrame.columns.map(colName => coalesce(dataFrame(colName), lit(value)).as(colName)): _*)
  }
}
