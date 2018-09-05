package enki

import cats._
import cats.implicits._
import freestyle.free.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe._

trait Database {
  /**
    * Since using SparkImplicits and SparkSession.implicits at once will lead to ambiguity SparkImplicits not imported
    * by default.
    */
  val implicits: SparkImplicits = new SparkImplicits {
    override def encoderStyle: EncoderStyle = Database.this.encoderStyle
  }

  def encoderStyle: EncoderStyle = EncoderStyle.Spark

  protected def writerSettings[F[_]](implicit writer: enki.DataFrameWriter[F]): writer.FS[Unit] = {
    ().pure[writer.FS]
  }

  def schema: String

  /* syntactic sugar */

  /* stages */

  final def dataFrame(rows: Seq[Row], schema: StructType): Stage[DataFrame] =
    enki.dataFrame(rows, schema)

  final def dataset[T: Encoder](data: Seq[T]): Stage[Dataset[T]] =
    enki.dataset(data, implicitly)

  final def read[T: Encoder](tableName: String, strict: Boolean = false): Stage[Dataset[T]] =
    enki.readDataset(schema, tableName, implicitly, strict)

  final def read(tableName: String): Stage[DataFrame] =
    enki.readDataFrame(schema, tableName)

  final def gread[T: TypeTag](tableName: String): Stage[Dataset[T]] =
    if (typeOf[T] == typeOf[Row]) {
      enki.readDataFrame(schema, tableName).asInstanceOf[Stage[Dataset[T]]]
    } else {
      enki.readDataset(schema, tableName, implicits.selectEncoder(ExpressionEncoder()), strict = false)
    }

  final def write[T: Encoder](tableName: String, strict: Boolean = false): Stage[Dataset[T] => Unit] =
    enki.writeDataset(schema, tableName, implicitly, strict, writerSettings)

  final def write(tableName: String): Stage[DataFrame => Unit] =
    enki.writeDataFrame(schema, tableName, writerSettings)

  final def gwrite[T: TypeTag](tableName: String): Stage[Dataset[T] => Unit] =
    if (typeOf[T] == typeOf[Row]) {
      enki.writeDataFrame(schema, tableName, writerSettings).asInstanceOf[Stage[Dataset[T] => Unit]]
    } else {
      enki.writeDataset(schema, tableName, implicits.selectEncoder(ExpressionEncoder()), strict = false, writerSettings)
    }

  final def arg[T: TypeTag](name: String, description: String = "", defaultValue: Option[T] = None): Stage[T] = {
    if (typeOf[T] == typeOf[Int]) {
      enki.integerArgument(name, description, defaultValue.asInstanceOf[Option[Int]]).asInstanceOf[Stage[T]]
    } else if (typeOf[T] == typeOf[String]) {
      enki.stringArgument(name, description, defaultValue.asInstanceOf[Option[String]]).asInstanceOf[Stage[T]]
    } else {
      throw new Exception(s"Arguments of type ${typeOf[T]} not supported.")
    }
  }

  /* program builder */

  final def persist[T: Encoder](tableName: String, stage: Stage[Dataset[T]], strict: Boolean = false): Program[Stage[Dataset[T]]] =
    enki.persistDataset(schema, tableName, stage, implicitly, strict, writerSettings)

  final def persist(tableName: String, stage: Stage[DataFrame]): Program[Stage[DataFrame]] =
    enki.persistDataFrame(schema, tableName, stage, writerSettings)

  final def gpersist[T: TypeTag](tableName: String, stage: Stage[Dataset[T]]): Program[Stage[Dataset[T]]] =
    if (typeOf[T] == typeOf[Row])
      enki
        .persistDataFrame(schema, tableName, stage.asInstanceOf[Stage[DataFrame]], writerSettings)
        .asInstanceOf[Program[Stage[Dataset[T]]]]
    else {
      enki.persistDataset(schema, tableName, stage, implicits.selectEncoder(ExpressionEncoder()), strict = false, writerSettings)
    }
}