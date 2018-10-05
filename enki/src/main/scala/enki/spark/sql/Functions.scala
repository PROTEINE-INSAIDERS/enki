package enki.spark.sql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

trait Functions extends DataTypeMappings {
  /**
    * Null column with type information.
    */
  //TODO: проверить, можно ли вместо него использовать TypedLit.
  def typedNull[T: DataTypeMapping]: Column = {
    lit(null).cast(implicitly[DataTypeMapping[T]].dataType)
  }
}
