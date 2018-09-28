package enki

import cats.implicits._
import freestyle.free.FreeS._
import freestyle.free._
import freestyle.free.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe._

trait Database[ProgramOp[_], StageOp[_]] {
  def schema: String

  def encoderStyle: EncoderStyle = EncoderStyle.Spark

  protected implicit val stageAlg: StageAlg[StageOp]
  protected implicit val programAlg: Program1[StageOp, ProgramOp]

  /**
    * Since using SparkImplicits and SparkSession.implicits at once will lead to ambiguity SparkImplicits not imported
    * by default.
    */
  val implicits: SparkImplicits = new SparkImplicits {
    override def encoderStyle: EncoderStyle = Database.this.encoderStyle
  }

  protected def writerSettings[T]: Par[StageOp, WriterSettings[T]] = WriterSettings[T]().pure[Par[StageOp, ?]]

  /* syntactic sugar */

  /* stages */

  final def dataFrame(
                       rows: Seq[Row],
                       schema: StructType
                     ): Par[StageOp, DataFrame] =
    stageAlg.dataFrame(rows, schema)

  // s.dataFrame(rows, schema)

  final def dataset[T: Encoder](data: Seq[T]): Par[StageOp, Dataset[T]] =
    stageAlg.dataset(data, implicitly)

  final def read(tableName: String): Par[StageOp, DataFrame] =
    stageAlg.readDataFrame(schema, tableName)

  final def read[T: Encoder](tableName: String, strict: Boolean = false): Par[StageOp, Dataset[T]] =
    stageAlg.readDataset(schema, tableName, implicitly, strict)

  final def gread[T: TypeTag](tableName: String): Par[StageOp, Dataset[T]] =
    if (typeOf[T] == typeOf[Row]) {
      stageAlg.readDataFrame(schema, tableName).asInstanceOf[Par[StageOp, Dataset[T]]]
    } else {
      stageAlg.readDataset[T](schema, tableName, implicits.selectEncoder(ExpressionEncoder()), strict = false)
    }

  final def write(tableName: String): Par[StageOp, DataFrame => Unit] = {
    writerSettings[Row].ap(stageAlg.writeDataFrame(schema, tableName))
  }

  final def write[T: Encoder](tableName: String, strict: Boolean = false): Par[StageOp, Dataset[T] => Unit] = {
    writerSettings[T].ap(stageAlg.writeDataset[T](schema, tableName, implicitly, strict))
  }

  final def gwrite[T: TypeTag](tableName: String): Par[StageOp, Dataset[T] => Unit] =
    if (typeOf[T] == typeOf[Row]) {
      writerSettings[Row].ap(stageAlg.writeDataFrame(schema, tableName)).asInstanceOf[Par[StageOp, Dataset[T] => Unit]]
    } else {
      writerSettings[T].ap(stageAlg.writeDataset[T](schema, tableName, implicits.selectEncoder(ExpressionEncoder()), strict = false))
    }

  /* program builder */

  final def persist[T: Encoder](
                                 tableName: String,
                                 dataset: Par[StageOp, Dataset[T]],
                                 strict: Boolean = false
                               ): Par[ProgramOp, Par[StageOp, Dataset[T]]] =
    programAlg.persistDataset(schema, tableName, dataset, implicitly, strict, this.writerSettings)

  final def persist(
                     tableName: String,
                     dataframe: Par[StageOp, DataFrame]
                   ): Par[ProgramOp, Par[StageOp, DataFrame]] =
    programAlg.persistDataFrame(schema, tableName, dataframe, writerSettings)


  final def gpersist[T: TypeTag](tableName: String, dataset: Par[StageOp, Dataset[T]]): Par[ProgramOp, Par[StageOp, Dataset[T]]] =
    if (typeOf[T] == typeOf[Row])
      programAlg
        .persistDataFrame(schema, tableName, dataset.asInstanceOf[Par[StageOp, DataFrame]], writerSettings)
        .asInstanceOf[Par[ProgramOp, Par[StageOp, Dataset[T]]]]
    else {
      programAlg.persistDataset[T](schema, tableName, dataset, implicits.selectEncoder(ExpressionEncoder()), strict = false, writerSettings)
    }
}