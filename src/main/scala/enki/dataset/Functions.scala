package enki.dataset

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, lit}

private [enki] object Functions {
  //TODO: добавить обработку NaN-ов как в pandas.
  def fillna(dataFrame: DataFrame, value: Any): DataFrame = {
    dataFrame.select(dataFrame.columns.map(colName => coalesce(dataFrame(colName), lit(value)).as(colName)): _*)
  }
}
