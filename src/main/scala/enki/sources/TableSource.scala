package enki
package sources

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders._

import scala.reflect.runtime.universe.{TypeTag, typeOf}

trait TableSource extends Source {
  protected def resolveTableName(name: Symbol): String

  override def read(name: Symbol, session: SparkSession): DataFrame = {
    session.table(resolveTableName(name))
  }

  override def decode[T: TypeTag](dataFrame: DataFrame): Dataset[T] = {
    if (typeOf[T] == typeOf[Row]) {
      dataFrame.asInstanceOf[Dataset[T]]
    } else {
      implicit val encoder: ExpressionEncoder[T] = ExpressionEncoder[T]()
      dataFrame.as[T]
    }
  }
}
