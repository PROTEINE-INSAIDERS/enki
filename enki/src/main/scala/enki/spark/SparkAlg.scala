package enki
package spark

import freestyle.free._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types._

@free trait SparkAlg {
  def arg(name: String): FS[String]

  def dataFrame(rows: Seq[Row], schema: StructType): FS[DataFrame]

  def dataset[T](data: Seq[T], encoder: Encoder[T]): FS[Dataset[T]]

  def readDataFrame(schemaName: String, tableName: String): FS[ReaderSettings => DataFrame]

  def readDataset[T](schemaName: String, tableName: String, encoder: Encoder[T], strict: Boolean): FS[ReaderSettings => Dataset[T]]

  //TODO: убрать трансформацию плана.
  def plan(plan: LogicalPlan): FS[PlanTransformer => DataFrame]

  def session: FS[SparkSession]

  def sql(sqlText: String): FS[PlanTransformer => DataFrame]

  def writeDataFrame(schemaName: String, tableName: String): FS[WriterSettings => DataFrame => Unit]

  def writeDataset[T](schemaName: String, tableName: String, encoder: Encoder[T], strict: Boolean): FS[WriterSettings => Dataset[T] => Unit]
}