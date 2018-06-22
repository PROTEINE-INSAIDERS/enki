package enki.constraints

import org.apache.spark.sql.{Column, DataFrame}

trait Constraints {
  def nonUnique(data: DataFrame, col: Column): DataFrame = {
    data.groupBy(col).count().where(col("count") > 1).select(col)
  }
}
