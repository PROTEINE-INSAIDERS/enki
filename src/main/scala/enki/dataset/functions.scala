package enki.dataset

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

private[enki] object functions {
  //TODO: добавить обработку NaN-ов как в pandas.
  def fillna(dataFrame: DataFrame, value: Any): DataFrame = {
    dataFrame.select(dataFrame.columns.map(colName => coalesce(dataFrame(colName), lit(value)).as(colName)): _*)
  }

  def diff(original: DataFrame, `new`: DataFrame, keyColumns: Seq[String]): DataFrame = {
    val diffStatusColName = "diff_status"
    val addedStatus = "added"
    val removedStatus = "removed"
    val updatedStatus = "updated"

    val added = keyColumns.map(original(_).isNull).reduce(_ and _)
    val removed = keyColumns.map(`new`(_).isNull).reduce(_ and _)

    def diffStatus(l: Column, r: Column): Column =
      when(added, addedStatus)
        .when(removed, removedStatus)
        .when(not(l <=> r), updatedStatus)

    def colDiff(name: String): Column = struct(
      original(name).as("old"),
      `new`(name).as("new"),
      diffStatus(original(name), `new`(name)).as(diffStatusColName)
    ).as(name)

    def rowStatus: Column =
      when(added, addedStatus)
        .when(removed, removedStatus)
        .when(not(original.schema.map(col => original(col.name) <=> `new`(col.name)).foldLeft(lit(true))(_ and _)), updatedStatus)

    original.join(`new`, keyColumns, "outer")
      .select(original.schema.map(c => colDiff(c.name)): _*)
      .withColumn(diffStatusColName, rowStatus)
  }
}
