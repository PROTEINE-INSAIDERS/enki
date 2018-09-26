package enki

import cats._
import cats.implicits._
import freestyle.free.FreeS._
import freestyle.free._
import freestyle.free.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe._

//TODO: define SimpleDatabase with default algebras.
//TODO: Используются 2 алгебры - для stage-ей и для программ. Возможно абстракция от алгебры программ не нужна,
// т.к программы нужны только для построения процесса вычисления из stage-ей, там фиксированная логика.
// Алгебра для stage-ей нужна, т.к. расширение функциональности реализовано через добавление кастомных stage.
trait Database[StageAlg[_], ProgramAlg[_]] {
  def schema: String

  def encoderStyle: EncoderStyle = EncoderStyle.Spark

  protected implicit val stage: Stage[StageAlg]
  protected implicit val program: Program1[StageAlg, ProgramAlg]

  /**
    * Since using SparkImplicits and SparkSession.implicits at once will lead to ambiguity SparkImplicits not imported
    * by default.
    */
  val implicits: SparkImplicits = new SparkImplicits {
    override def encoderStyle: EncoderStyle = Database.this.encoderStyle
  }

  protected def writerSettings[T]: Par[StageAlg, WriterSettings[T]] = WriterSettings[T]().pure[Par[StageAlg, ?]]

  /* syntactic sugar */

  /* stages */

  final def dataFrame(rows: Seq[Row], schema: StructType): Par[StageAlg, DataFrame] =
    stage.dataFrame(rows, schema)

  final def dataset[T: Encoder](data: Seq[T]): Par[StageAlg, Dataset[T]] =
    stage.dataset(data, implicitly)

  final def read(tableName: String): Par[StageAlg, DataFrame] =
    stage.readDataFrame(schema, tableName)

  final def read[T: Encoder](tableName: String, strict: Boolean = false): Par[StageAlg, Dataset[T]] =
    stage.readDataset(schema, tableName, implicitly, strict)

  final def gread[T: TypeTag](tableName: String): Par[StageAlg, Dataset[T]] =
    if (typeOf[T] == typeOf[Row]) {
      stage.readDataFrame(schema, tableName).asInstanceOf[stage.FS[Dataset[T]]]
    } else {
      stage.readDataset[T](schema, tableName, implicits.selectEncoder(ExpressionEncoder()), strict = false)
    }

  final def write(tableName: String): Par[StageAlg, DataFrame => Unit] = {
    writerSettings[Row].ap(stage.writeDataFrame(schema, tableName))
  }

  final def write[T: Encoder](tableName: String, strict: Boolean = false): Par[StageAlg, Dataset[T] => Unit] = {
    writerSettings[T].ap(stage.writeDataset[T](schema, tableName, implicitly, strict))
  }

  final def gwrite[T: TypeTag](tableName: String): Par[StageAlg, Dataset[T] => Unit] =
    if (typeOf[T] == typeOf[Row]) {
      writerSettings[Row].ap(stage.writeDataFrame(schema, tableName)).asInstanceOf[Par[StageAlg, Dataset[T] => Unit]]
    } else {
      writerSettings[T].ap(stage.writeDataset[T](schema, tableName, implicits.selectEncoder(ExpressionEncoder()), strict = false))
    }

  final def persist[T: Encoder](
                                 tableName: String,
                                 dataset: Par[StageAlg, Dataset[T]],
                                 strict: Boolean = false
                               ): Par[ProgramAlg, Par[StageAlg, Dataset[T]]] =
    program.persistDataset(schema, tableName, dataset, implicitly, strict, this.writerSettings)

  final def persist(
                     tableName: String,
                     dataframe: Par[StageAlg, DataFrame]
                   ): Par[ProgramAlg, Par[StageAlg, DataFrame]]  =
    program.persistDataFrame(schema, tableName, dataframe, writerSettings)

  /* program builder */
/*
  final def gpersist[T: TypeTag](tableName: String, stage: FreeS.Par[Stage.Op, Dataset[T]]): Program[FreeS.Par[Stage.Op, Dataset[T]]] =
    if (typeOf[T] == typeOf[Row])
      enki
        .persistDataFrame(schema, tableName, stage.asInstanceOf[FreeS.Par[Stage.Op, DataFrame]], writerSettings)
        .asInstanceOf[Program[FreeS.Par[Stage.Op, Dataset[T]]]]
    else {
      enki.persistDataset(schema, tableName, stage, implicits.selectEncoder(ExpressionEncoder()), strict = false, writerSettings)
    }
    */
}