package enki.dataset

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

//TODO: сделать trait?
private[enki] object functions {
  //TODO: добавить обработку NaN-ов как в pandas.
  def fillna(dataFrame: DataFrame, value: Any): DataFrame = {
    dataFrame.select(dataFrame.columns.map(colName => coalesce(dataFrame(colName), lit(value)).as(colName)): _*)
  }

  private val diffStatusColName = "diff_status"
  private val addedStatus = "added"
  private val removedStatus = "removed"
  private val updatedStatus = "updated"
  private val oldValue = "old"
  private val newValue = "new"

  def diff(original: DataFrame, `new`: DataFrame, keyColumns: Seq[String]): DataFrame = {

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
    )

    def rowStatus: Column =
      when(added, addedStatus)
        .when(removed, removedStatus)
        .when(not(original.schema.map(col => original(col.name) <=> `new`(col.name)).foldLeft(lit(true))(_ and _)), updatedStatus)

    original.join(`new`, keyColumns.map { c => original(c) === `new`(c) }.reduce(_ and _), "outer")
      .select(rowStatus.as(diffStatusColName) +: original.schema.map(c => colDiff(c.name).as(c.name)): _*)
  }

  /**
    * Convert diff frame to format similar to http://paulfitz.github.io/dataprotocols/tabular-diff-format/
    * TODO: механизм усранения коллизий по -> не реализован
    */
  def formatDiff(diff: DataFrame): DataFrame = {
    def formatActionColumn: Column =
      when(diff(diffStatusColName) === addedStatus, "+++")
        .when(diff(diffStatusColName) === removedStatus, "---")
        .when(diff(diffStatusColName) === updatedStatus, "->")

    def formatDiffColumn(colName: String): Column =
      when(diff(s"$colName.$diffStatusColName") === addedStatus, diff(s"$colName.$newValue"))
        .when(diff(s"$colName.$diffStatusColName") === removedStatus, diff(s"$colName.$oldValue"))
        .when(diff(s"$colName.$diffStatusColName") === updatedStatus, concat(diff(s"$colName.$oldValue"), lit("->"), diff(s"$colName.$newValue")))
        .otherwise(diff(s"$colName.$oldValue"))

    diff.select(
      formatActionColumn.as(diffStatusColName) +: diff.schema.filter(_.name != diffStatusColName).map(c => formatDiffColumn(c.name).as(c.name)): _*)
  }
}
