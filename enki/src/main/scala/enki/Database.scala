package enki

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

  //TODO: у нас имеются несколько уровней настройки параметров сохранения: таблица <- база данных <- параметры спарка по-умолчанию.
  // на каждом из уровней должна быть возможность перегрузить настройки, либо унаследовать их. Возможно для этого
  // подойдёт HList. Если настройка присутсвует в HList-e, берем её, иначе наследуем (HList обеспечивает строгую типизацию
  // по сравнению со словарём, с другой стороны иных преимуществ он не даёт).
  protected def saveMode: Option[SaveMode] = None

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
    enki.writeDataset(schema, tableName, implicitly, strict, saveMode)

  final def write(tableName: String): Stage[DataFrame => Unit] =
    enki.writeDataFrame(schema, tableName, saveMode)

  final def gwrite[T: TypeTag](tableName: String): Stage[Dataset[T] => Unit] =
    if (typeOf[T] == typeOf[Row]) {
      enki.writeDataFrame(schema, tableName, saveMode).asInstanceOf[Stage[Dataset[T] => Unit]]
    } else {
      enki.writeDataset(schema, tableName, implicits.selectEncoder(ExpressionEncoder()), strict = false, saveMode)
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
    enki.persistDataset(schema, tableName, stage, implicitly, strict, saveMode)

  final def persist(tableName: String, stage: Stage[DataFrame]): Program[Stage[DataFrame]] =
    enki.persistDataFrame(schema, tableName, stage, saveMode)

  final def gpersist[T: TypeTag](tableName: String, stage: Stage[Dataset[T]]): Program[Stage[Dataset[T]]] =
    if (typeOf[T] == typeOf[Row])
      enki
        .persistDataFrame(schema, tableName, stage.asInstanceOf[Stage[DataFrame]], saveMode)
        .asInstanceOf[Program[Stage[Dataset[T]]]]
    else {
      enki.persistDataset(schema, tableName, stage, implicits.selectEncoder(ExpressionEncoder()), strict = false, saveMode)
    }
}