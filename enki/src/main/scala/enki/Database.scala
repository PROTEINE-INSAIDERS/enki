package enki

import cats.implicits._
import freestyle.free.FreeS._
import freestyle.free.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe._

//TODO: разбить на отдельные трейты для операций над спарком, программой и аргументами.
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
    alg.readDataFrame(schema, tableName)

  final def read[T: Encoder](tableName: String, strict: Boolean = false)(implicit alg: SparkAlg[StageOp]): alg.FS[Dataset[T]] =
    alg.readDataset(schema, tableName, implicitly, strict)

  final def gread[T: TypeTag](tableName: String)(implicit alg: SparkAlg[StageOp]): alg.FS[Dataset[T]] =
    if (typeOf[T] == typeOf[Row]) {
      alg.readDataFrame(schema, tableName).asInstanceOf[alg.FS[Dataset[T]]]
    } else {
      alg.readDataset[T](schema, tableName, implicits.selectEncoder(ExpressionEncoder()), strict = false)
    }

  final def write(tableName: String)(implicit alg: SparkAlg[StageOp]): alg.FS[DataFrame => Unit] = {
    writerSettings.ap(alg.writeDataFrame(schema, tableName))
  }

  final def write[T: Encoder](tableName: String, strict: Boolean = false)(implicit alg: SparkAlg[StageOp]): alg.FS[Dataset[T] => Unit] = {
    writerSettings.ap(alg.writeDataset[T](schema, tableName, implicitly, strict))
  }

  final def gwrite[T: TypeTag](tableName: String)(implicit alg: SparkAlg[StageOp]): alg.FS[Dataset[T] => Unit] =
    if (typeOf[T] == typeOf[Row]) {
      writerSettings.ap(alg.writeDataFrame(schema, tableName)).asInstanceOf[Par[StageOp, Dataset[T] => Unit]]
    } else {
      writerSettings.ap(alg.writeDataset[T](schema, tableName, implicits.selectEncoder(ExpressionEncoder()), strict = false))
    }

  /* program builder */

  final def persist[T: Encoder](
                                 tableName: String,
                                 dataset: Par[StageOp, Dataset[T]],
                                 strict: Boolean = false
                               )(implicit alg: StageOpProvider[StageOp]#ProgramAlg[ProgramOp]): alg.FS[Par[StageOp, Dataset[T]]] =
    alg.persistDataset(schema, tableName, dataset, implicitly, strict, this.writerSettings)

  final def persist(
                     tableName: String,
                     dataframe: Par[StageOp, DataFrame]
                   )(implicit alg: StageOpProvider[StageOp]#ProgramAlg[ProgramOp]): alg.FS[Par[StageOp, DataFrame]] =
    alg.persistDataFrame(schema, tableName, dataframe, writerSettings)

  final def gpersist[T: TypeTag](
                                  tableName: String,
                                  dataset: Par[StageOp, Dataset[T]]
                                )(implicit alg: StageOpProvider[StageOp]#ProgramAlg[ProgramOp]): alg.FS[Par[StageOp, Dataset[T]]] =
    if (typeOf[T] == typeOf[Row])
      alg
        .persistDataFrame(schema, tableName, dataset.asInstanceOf[Par[StageOp, DataFrame]], writerSettings)
        .asInstanceOf[Par[ProgramOp, Par[StageOp, Dataset[T]]]]
    else {
      alg.persistDataset[T](schema, tableName, dataset, implicits.selectEncoder(ExpressionEncoder()), strict = false, writerSettings)
    }
}