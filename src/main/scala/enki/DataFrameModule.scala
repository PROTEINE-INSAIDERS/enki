package enki

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

trait DataFrameModule {
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

    //TODO: найти лучший способ работать с таблицами, у которых не совпадают схемы.
    val columns = (original.schema.map(_.name).toSet intersect `new`.schema.map(_.name).toSet).toSeq

    def rowStatus: Column =
      when(added, addedStatus)
        .when(removed, removedStatus)
        .when(not(columns.map(col => original(col) <=> `new`(col)).foldLeft(lit(true))(_ and _)), updatedStatus)

    original.join(`new`, keyColumns.map { c => original(c) === `new`(c) }.reduce(_ and _), "outer")
      .select(rowStatus.as(diffStatusColName) +: columns.map(c => colDiff(c).as(c)): _*)
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

  def fillna(dataFrame: DataFrame, value: Any): DataFrame = {
    dataFrame.select(dataFrame.columns.map(colName => coalesce(dataFrame(colName), lit(value)).as(colName)): _*)
  }

  def nonUnique(data: DataFrame, cols: Seq[Column]): DataFrame = {
    data.groupBy(cols: _*).count().where(col("count") > 1).select(cols: _*)
  }

  implicit class DataFrameExtensions(dataFrame: DataFrame) {
    /**
      * Diff current dataset against other.
      *
      * "Current" dataset considered "original" or "left" in diff operation.
      */
    def diff(other: DataFrame, keyColumns: Seq[String]): DataFrame = {
      DataFrameModule.this.diff(dataFrame, other, keyColumns)
    }

    def fillna(value: Any): DataFrame = {
      DataFrameModule.this.fillna(dataFrame, value)
    }

    def nonUniq(col: Column*): DataFrame = {
      DataFrameModule.this.nonUnique(dataFrame, col)
    }
  }
}
