package enki.readers

import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._

import scala.reflect.runtime.universe.{TypeTag, typeOf}

trait EmptyReader extends Reader with SchemaProvider {
  override def getSchema(name: Symbol): Option[StructType] = None

  override def read[T: TypeTag](name: Symbol, session: SparkSession): Dataset[T] = {
    if (typeOf[T] == typeOf[Row]) {
      val schema = getSchema(name).getOrElse(throw new Exception(s"Unable to infer schema for table ${name.name}"))
      session.createDataFrame(session.sparkContext.emptyRDD[Row], schema).asInstanceOf[Dataset[T]]
    } else {
      implicit val encoder: Encoder[T] = ExpressionEncoder[T]()
      val schema = getSchema(name).getOrElse(encoder.schema)
      session.createDataFrame(session.sparkContext.emptyRDD[Row], schema).as[T]
    }
  }
}
