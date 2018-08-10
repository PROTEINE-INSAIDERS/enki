package enki

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe.TypeTag

trait Database {
  def schema: String

  //TODO: у нас имеются несколько уровней настройки параметров сохранения: таблица <- база данных <- параметры спарка по-умолчанию.
  // на каждом из уровней должна быть возможность перегрузить настройки, либо унаследовать их. Возможно для этого
  // подойдёт HList. Если настройка присутсвует в HList-e, берем её, иначе наследуем (HList обеспечивает строгую типизацию
  // по сравнению со словарём, с другой стороны иных преимуществ он не даёт).
  protected def saveMode: Option[SaveMode] = None

  /* syntactic sugar */

  final def dataFrame(rows: Seq[Row], schema: StructType): Stage[DataFrame] =
    enki.dataFrame(rows, schema)

  final def dataset[T: TypeTag](data: Seq[T]): Stage[Dataset[T]] =
    enki.dataset(data)

  final def read[T: TypeTag](tableName: String, strict: Boolean = false): Stage[Dataset[T]] =
    enki.read[T](schema, tableName, strict)

  final def write[T: TypeTag](tableName: String, strict: Boolean = false): Stage[Dataset[T] => Unit] =
    enki.write(schema, tableName, strict, saveMode)
   
  final def persist[T: TypeTag](tableName: String, stage: Stage[Dataset[T]], strict: Boolean = false): Program[Stage[Dataset[T]]] =
    enki.persist(schema, tableName, stage, strict, saveMode)
}
