package enki
package sources

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders._

import scala.reflect.runtime.universe.{TypeTag, typeOf}

trait TableSource extends Source {
  protected def resolveTableName(name: Symbol): String

  protected def decode[T: TypeTag](dataFrame: DataFrame): Dataset[T] = {
    if (typeOf[T] == typeOf[Row]) {
      dataFrame.asInstanceOf[Dataset[T]]
    } else {
      implicit val encoder: ExpressionEncoder[T] = ExpressionEncoder[T]()
      dataFrame.as[T]
    }
  }

  override def read[T: TypeTag](name: Symbol, session: SparkSession): Dataset[T] = {
    decode[T](session.table(resolveTableName(name)))
  }
}
