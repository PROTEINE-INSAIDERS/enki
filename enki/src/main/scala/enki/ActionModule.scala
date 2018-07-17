package enki

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.types._
import scala.collection.JavaConversions._

import scala.reflect.runtime.universe.{TypeTag, typeOf}

//TODO: Возможно следует убрать этот модуль, actions можно определять по месту использования.
trait ActionModule {

  type SparkAction[A] = SparkSession => A

  protected def datasetAction[T: TypeTag](data: Seq[T]): SparkAction[Dataset[T]] = session =>
    session.createDataset(data)(ExpressionEncoder())

  protected def dataFrameAction(rows: Seq[Row], schema: StructType): SparkAction[DataFrame] = session =>
    session.createDataFrame(rows, schema)

  protected def readAction[T: TypeTag](database: Database, table: String): SparkAction[Dataset[T]] = session => {
    if (typeOf[T] == typeOf[Row])
      database.readTable(session, table).asInstanceOf[Dataset[T]]
    else
      database.readTable(session, table).as[T](ExpressionEncoder())
  }

  protected def writeAction(database: Database, table: String): SparkAction[Dataset[_] => Unit] = _ => data =>
    database.writeTable(table, data.toDF())
}
