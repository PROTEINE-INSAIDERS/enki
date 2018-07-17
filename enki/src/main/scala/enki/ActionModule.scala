package enki

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders._

import scala.reflect.runtime.universe.{TypeTag, typeOf}

//TODO: возможно это следует объединить с функциональностью Database в одном модуле.
trait ActionModule {

  type SparkAction[A] = SparkSession => A

  protected def datasetAction[T: TypeTag](data: Seq[T]): SparkAction[Dataset[T]] = session =>
    session.createDataset(data)(ExpressionEncoder())

  protected def readAction[T: TypeTag](database: Database, table: String): SparkAction[Dataset[T]] = session => {
    if (typeOf[T] == typeOf[Row])
      database.readTable(session, table).asInstanceOf[Dataset[T]]
    else
      database.readTable(session, table).as[T](ExpressionEncoder())
  }

  protected def writeAction(database: Database, table: String): SparkAction[Dataset[_] => Unit] = _ => data =>
    database.writeTable(table, data.toDF())
}
