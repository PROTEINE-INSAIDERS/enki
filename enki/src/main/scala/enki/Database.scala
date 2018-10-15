package enki

import cats.implicits._
import enki.spark.ReaderSettings
import freestyle.free.FreeS._
import freestyle.free.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe._

//TODO: разбить на отдельные трейты для операций над спарком, программой и аргументами.
//TODO: реализовать партиционирование.
//  для реализации схем партиционирования метод persist должен принимать аргументы партиционирования, модифицировать
//  writerSettings для writer-а и добавлять фильтр для команды чтения.
// возможно имеет смысл сделать отдельную команду persist partition.
trait Database {
  val enki: Enki

  def schema: String

  def encoderStyle: EncoderStyle = EncoderStyle.Spark

  type ProgramOp[_]
  type StageOp[_]

  /**
    * Since using SparkImplicits and SparkSession.implicits at once will lead to ambiguity SparkImplicits not imported
    * by default.
    */
  val implicits: SparkImplicits = new SparkImplicits {
    override def encoderStyle: EncoderStyle = Database.this.encoderStyle
  }

  protected def readerSettings: Par[StageOp, ReaderSettings] = ReaderSettings().pure[Par[StageOp, ?]]

  protected def writerSettings: Par[StageOp, WriterSettings] = WriterSettings().pure[Par[StageOp, ?]]

  /* syntactic sugar */

  /* stages */

  final def dataFrame(
                       rows: Seq[Row],
                       schema: StructType
                     )(
                       implicit alg: SparkAlg[StageOp]
                     ): alg.FS[DataFrame] =
    alg.dataFrame(rows, schema)

  // s.dataFrame(rows, schema)

  final def dataset[T: Encoder](
                                 data: Seq[T]
                               )
                               (
                                 implicit alg: SparkAlg[StageOp]
                               ): alg.FS[Dataset[T]] =
    alg.dataset(data, implicitly)

  final def read(tableName: String)(implicit alg: SparkAlg[StageOp]): alg.FS[DataFrame] =
    alg.readDataFrame(schema, tableName) <*> readerSettings

  final def read[T: Encoder](tableName: String, strict: Boolean = false)(implicit alg: SparkAlg[StageOp]): alg.FS[Dataset[T]] =
    alg.readDataset(schema, tableName, implicitly[Encoder[T]], strict) <*> readerSettings

  final def gread[T: TypeTag](tableName: String)(implicit alg: SparkAlg[StageOp]): alg.FS[Dataset[T]] =
    if (typeOf[T] == typeOf[Row]) {
      (alg.readDataFrame(schema, tableName) <*> readerSettings).asInstanceOf[alg.FS[Dataset[T]]]
    } else {
      alg.readDataset[T](schema, tableName, implicits.selectEncoder(ExpressionEncoder()), strict = false) <*> readerSettings
    }

  final def sql(sqlSting: String)(implicit alg: SparkAlg[StageOp]): alg.FS[DataFrame] = alg.sql(sqlSting)

  final def write(
                   tableName: String
                 )
                 (
                   implicit alg: SparkAlg[StageOp]
                 ): alg.FS[DataFrame => Unit] = write(tableName, writerSettings)

  //TODO: нужны ли нам перегрузки с возможностью задания writerSettings?
  final def write(
                   tableName: String,
                   writerSettings: Par[StageOp, WriterSettings]
                 )
                 (
                   implicit alg: SparkAlg[StageOp]
                 ): alg.FS[DataFrame => Unit] = {
    alg.writeDataFrame(schema, tableName) <*> writerSettings
  }


  final def write[T: Encoder](tableName: String, strict: Boolean = false)(implicit alg: SparkAlg[StageOp]): alg.FS[Dataset[T] => Unit] = {
    alg.writeDataset[T](schema, tableName, implicitly, strict) <*> writerSettings
  }

  final def gwrite[T: TypeTag](tableName: String)(implicit alg: SparkAlg[StageOp]): alg.FS[Dataset[T] => Unit] =
    if (typeOf[T] == typeOf[Row]) {
      (alg.writeDataFrame(schema, tableName) <*> writerSettings).asInstanceOf[alg.FS[Dataset[T] => Unit]]
    } else {
      alg.writeDataset[T](schema, tableName, implicits.selectEncoder(ExpressionEncoder()), strict = false) <*> writerSettings
    }

  /* program builder */

  final def persist[T: Encoder](
                                 tableName: String,
                                 dataset: Par[StageOp, Dataset[T]],
                                 strict: Boolean = false
                               )(implicit alg: StageOpProvider[StageOp]#ProgramAlg[ProgramOp]): alg.FS[Par[StageOp, Dataset[T]]] =
    alg.persistDataset(schema, tableName, dataset, implicitly, strict, readerSettings, writerSettings)

  final def persist(
                     tableName: String,
                     dataframe: Par[StageOp, DataFrame]
                   )(implicit alg: StageOpProvider[StageOp]#ProgramAlg[ProgramOp]): alg.FS[Par[StageOp, DataFrame]] =
    alg.persistDataFrame(schema, tableName, dataframe, readerSettings, writerSettings)

  final def gpersist[T: TypeTag](
                                  tableName: String,
                                  dataset: Par[StageOp, Dataset[T]]
                                )(implicit alg: StageOpProvider[StageOp]#ProgramAlg[ProgramOp]): alg.FS[Par[StageOp, Dataset[T]]] =
    if (typeOf[T] == typeOf[Row])
      alg
        .persistDataFrame(schema, tableName, dataset.asInstanceOf[Par[StageOp, DataFrame]], readerSettings, writerSettings)
        .asInstanceOf[Par[ProgramOp, Par[StageOp, Dataset[T]]]]
    else {
      alg.persistDataset[T](schema, tableName, dataset, implicits.selectEncoder(ExpressionEncoder()), strict = false, readerSettings, writerSettings)
    }
}