package enki

import org.apache.spark.sql._
import org.apache.spark.sql.types._

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

  final def dataFrame(rows: Seq[Row], schema: StructType): Stage[DataFrame] =
    enki.dataFrame(rows, schema)

  final def dataset[T: Encoder](data: Seq[T]): Stage[Dataset[T]] =
    enki.dataset(data, implicitly)

  final def read[T: Encoder](tableName: String, strict: Boolean = false): Stage[Dataset[T]] =
    enki.readDataset[T](schema, tableName, implicitly, strict)

  final def read(tableName: String): Stage[DataFrame] =
    enki.readDataFrame(schema, tableName)

  final def write[T: Encoder](tableName: String, strict: Boolean = false): Stage[Dataset[T] => Unit] =
    enki.writeDataset(schema, tableName, implicitly, strict, saveMode)

  final def write(tableName: String): Stage[DataFrame => Unit] =
    enki.writeDataFrame(schema, tableName, saveMode)

  final def persist[T: Encoder](tableName: String, stage: Stage[Dataset[T]], strict: Boolean = false): Program[Stage[Dataset[T]]] =
    enki.persistDataset(schema, tableName, stage, implicitly, strict, saveMode)

  final def persist(tableName: String, stage: Stage[DataFrame]): Program[Stage[DataFrame]] =
    enki.persistDataFrame(schema, tableName, stage, saveMode)
}
