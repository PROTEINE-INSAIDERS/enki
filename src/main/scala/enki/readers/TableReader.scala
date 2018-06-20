package enki
package readers

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders._

import scala.reflect.runtime.universe.{TypeTag, typeOf}

trait TableReader extends Reader {
  protected def getTableName(name: Symbol): String

  protected def decode[T: TypeTag](dataFrame: DataFrame): Dataset[T] = {
    if (typeOf[T] == typeOf[Row]) {
      dataFrame.asInstanceOf[Dataset[T]]
    } else {
      implicit val encoder: ExpressionEncoder[T] = ExpressionEncoder[T]()
      dataFrame.as[T]
    }
  }

  override def read[T: TypeTag](table: Symbol, session: SparkSession): Dataset[T] = {
    decode[T](session.table(getTableName(table)))
  }
}
