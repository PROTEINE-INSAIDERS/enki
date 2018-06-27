package enki
package writers

import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag


trait TableWriter extends Writer {
  protected def tableName(table: Symbol): String

  protected def saveMode: SaveMode = SaveMode.ErrorIfExists

  protected def encode[T: TypeTag](data: Dataset[T]): DataFrame = data.toDF()

  override def write[T: TypeTag](table: Symbol, data: Dataset[T], session: SparkSession): Unit = {
    val name = tableName(table)
    session.sparkContext.setJobDescription(s"INSERT ${if (saveMode == SaveMode.Overwrite) "OVERWRITE" else "INTO"} TABLE $name")
    try {
      data.write.mode(saveMode).saveAsTable(name)
    } finally {
      session.sparkContext.setJobDescription(null)
    }
  }
}
