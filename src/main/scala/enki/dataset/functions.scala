package enki.dataset

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

private[enki] object functions {
  //TODO: добавить обработку NaN-ов как в pandas.
  def fillna(dataFrame: DataFrame, value: Any): DataFrame = {
    dataFrame.select(dataFrame.columns.map(colName => coalesce(dataFrame(colName), lit(value)).as(colName)): _*)
  }

  def diff(l: DataFrame, r: DataFrame, keyColumns: Seq[String]): DataFrame = {
    val diffStatus = "diff_status"
    val addedStatus = "+++"
    val removedStatus = "---"
    val updatedStatus = "->"

    //TODO:
    // 1. найти новые записи
    // 2. найти станые записи
    // 3. найти записи, у которых изменились значения.

    val added = keyColumns.map(l(_).isNull).reduce(_ and _)
    val removed = keyColumns.map(r(_).isNull).reduce(_ and _)

    def diffStatus(l: Column, r: Column): Column =
      when(added, addedStatus)
        .when(removed, removedStatus)
        .when(not(l <=> r), updatedStatus)

    def colDiff(name: String): Column = struct(
      l(name).as("old"),
      r(name).as("new"),
      diffStatus(l(name), r(name)).as(diffStatus)
    ).as(name)


    old
      .join(other, keyColumns, "outer")
    //  .select()


    ???
  }
}
