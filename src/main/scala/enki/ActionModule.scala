package enki

trait ActionModule {

  import org.apache.spark.sql._
  import org.apache.spark.sql.catalyst.encoders._

  import scala.reflect.runtime.universe.{TypeTag, typeOf}

  type SparkAction[A] = SparkSession => A

  protected def readAction[T: TypeTag](db: Database, table: String): SparkAction[Dataset[T]] = session => {
    if (typeOf[T] == typeOf[Row])
      db.readTable(session, table).asInstanceOf[Dataset[T]]
    else
      db.readTable(session, table).as[T](ExpressionEncoder())
  }
}
