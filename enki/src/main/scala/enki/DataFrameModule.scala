package enki

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait DataFrameModule {

  private val diffStatusColName = "diff_status"
  private val addedStatus = "added"
  private val removedStatus = "removed"
  private val updatedStatus = "updated"
  private val oldValue = "old"
  private val newValue = "new"

  def diff(l: DataFrame,
           r: DataFrame,
           keyColumns: Seq[String],
           withEpsilon: Boolean): DataFrame = {

    val added = keyColumns.map(l(_).isNull).reduce(_ and _)
    val removed = keyColumns.map(r(_).isNull).reduce(_ and _)

    //TODO: найти лучший способ работать с таблицами, у которых не совпадают схемы.
    val columns = (l.schema.map(_.name).toSet intersect r.schema.map(_.name).toSet).toSeq

    def floatSafeEq[T](l: Column, r: Column, e: T): Column =
      (l.isNull && r.isNull) or not(abs(l - r) > e)

    def different(name: String) = not(l.schema(name).dataType match {
      case FloatType if withEpsilon => floatSafeEq(l(name), r(name), Float.MinPositiveValue)
      case DoubleType if withEpsilon => floatSafeEq(l(name), r(name), Double.MinPositiveValue)
      case _ => l(name) <=> r(name)
    })

    def colDiff(name: String): Column = struct(
      l(name).as(oldValue),
      r(name).as(newValue),
      when(added, addedStatus)
        .when(removed, removedStatus)
        .when(not(l(name) <=> r(name)), updatedStatus).as(diffStatusColName)
    )

    def rowStatus: Column =
      when(added, addedStatus)
        .when(removed, removedStatus)
        .when(not(columns.map(col => l(col) <=> r(col)).foldLeft(lit(true))(_ and _)), updatedStatus)

    l
      .join(r, keyColumns.map { c => l(c) === r(c) }.reduce(_ and _), "outer")
      .select(columns.map(c => colDiff(c).as(c)): _*)
      .withColumn(diffStatusColName, coalesce(columns.map(c => col(s"$c.$diffStatusColName")): _*))
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

  def nonUniq(data: DataFrame, cols: Seq[Column]): DataFrame = {
    data
      .withColumn("__count", count("*").over(partitionBy(cols: _*)))
      .where(col("__count") > 1)
      .drop("__count")
  }

  implicit class DataFrameExtensions(dataFrame: DataFrame) {
    /**
      * Diff current dataset against other.
      *
      * "Current" dataset considered "original" or "left" in diff operation.
      */
    def diff(other: DataFrame, keyColumns: Seq[String], withEpsilon: Boolean = false): DataFrame = {
      DataFrameModule.this.diff(dataFrame, other, keyColumns, withEpsilon)
    }

    def fillna(value: Any): DataFrame = {
      DataFrameModule.this.fillna(dataFrame, value)
    }

    def nonUniq(col: Column*): DataFrame = {
      DataFrameModule.this.nonUniq(dataFrame, col)
    }
  }

}
