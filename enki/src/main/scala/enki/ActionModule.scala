package enki

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe.{TypeTag, typeOf}

//TODO: Возможно следует убрать этот модуль, actions можно определять по месту использования.
trait ActionModule {

  protected def datasetAction[T: TypeTag](data: Seq[T]): SparkAction[Dataset[T]] = session =>
    session.createDataset(data)(ExpressionEncoder())

  protected def dataFrameAction(rows: Seq[Row], schema: StructType): SparkAction[DataFrame] = session =>
    session.createDataFrame(rows, schema)

  protected def writeAction(database: Database, table: String): SparkAction[Dataset[_] => Unit] = _ => data =>
    database.writeTable(table, data.toDF())
}
