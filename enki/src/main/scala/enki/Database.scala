package enki

import cats.implicits._
import freestyle.free.FreeS._
import freestyle.free.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe._

trait Database[ProgramOp[_], StageOp[_]] {
  def schema: String

  def encoderStyle: EncoderStyle = EncoderStyle.Spark

  protected val argsAlg: ArgsAlg[StageOp]
  protected val stageAlg: StageAlg[StageOp]
  protected val programAlg: Program1[StageOp, ProgramOp]

  /**
    * Since using SparkImplicits and SparkSession.implicits at once will lead to ambiguity SparkImplicits not imported
    * by default.
    */
  val implicits: SparkImplicits = new SparkImplicits {
    override def encoderStyle: EncoderStyle = Database.this.encoderStyle
  }

  protected def writerSettings: stageAlg.FS[WriterSettings] = WriterSettings().pure[stageAlg.FS]

  /* syntactic sugar */

  /* stages */

  final def dataFrame(
                       rows: Seq[Row],
                       schema: StructType
                     ): stageAlg.FS[DataFrame] =
    stageAlg.dataFrame(rows, schema)

  // s.dataFrame(rows, schema)

  final def dataset[T: Encoder](data: Seq[T]): stageAlg.FS[Dataset[T]] =
    stageAlg.dataset(data, implicitly)

  final def read(tableName: String): stageAlg.FS[DataFrame] =
    stageAlg.readDataFrame(schema, tableName)

  final def read[T: Encoder](tableName: String, strict: Boolean = false): stageAlg.FS[Dataset[T]] =
    stageAlg.readDataset(schema, tableName, implicitly, strict)

  final def gread[T: TypeTag](tableName: String): stageAlg.FS[Dataset[T]] =
    if (typeOf[T] == typeOf[Row]) {
      stageAlg.readDataFrame(schema, tableName).asInstanceOf[stageAlg.FS[Dataset[T]]]
    } else {
      stageAlg.readDataset[T](schema, tableName, implicits.selectEncoder(ExpressionEncoder()), strict = false)
    }

  final def write(tableName: String): stageAlg.FS[DataFrame => Unit] = {
    writerSettings.ap(stageAlg.writeDataFrame(schema, tableName))
  }

  final def write[T: Encoder](tableName: String, strict: Boolean = false): stageAlg.FS[Dataset[T] => Unit] = {
    writerSettings.ap(stageAlg.writeDataset[T](schema, tableName, implicitly, strict))
  }

  final def gwrite[T: TypeTag](tableName: String): stageAlg.FS[Dataset[T] => Unit] =
    if (typeOf[T] == typeOf[Row]) {
      writerSettings.ap(stageAlg.writeDataFrame(schema, tableName)).asInstanceOf[Par[StageOp, Dataset[T] => Unit]]
    } else {
      writerSettings.ap(stageAlg.writeDataset[T](schema, tableName, implicits.selectEncoder(ExpressionEncoder()), strict = false))
    }

  /* arguments */

  final def arg[T: TypeTag](name: String, description: String = "", defaultValue: Option[T] = None): argsAlg.FS[T] = {
    if (typeOf[T] == typeOf[Boolean]) {
      argsAlg.bool(name, description, defaultValue.asInstanceOf[Option[Boolean]]).asInstanceOf[argsAlg.FS[T]]
    } else if (typeOf[T] == typeOf[Int]) {
      argsAlg.int(name, description, defaultValue.asInstanceOf[Option[Int]]).asInstanceOf[argsAlg.FS[T]]
    } else if (typeOf[T] == typeOf[String]) {
      argsAlg.string(name, description, defaultValue.asInstanceOf[Option[String]]).asInstanceOf[argsAlg.FS[T]]
    } else {
      throw new Exception(s"Argument of type ${typeOf[T]} not supported.")
    }
  }

  /* program builder */

  final def persist[T: Encoder](
                                 tableName: String,
                                 dataset: Par[StageOp, Dataset[T]],
                                 strict: Boolean = false
                               ): programAlg.FS[stageAlg.FS[Dataset[T]]] =
    programAlg.persistDataset(schema, tableName, dataset, implicitly, strict, this.writerSettings)

  final def persist(
                     tableName: String,
                     dataframe: Par[StageOp, DataFrame]
                   ): programAlg.FS[stageAlg.FS[DataFrame]] =
    programAlg.persistDataFrame(schema, tableName, dataframe, writerSettings)


  final def gpersist[T: TypeTag](
                                  tableName: String,
                                  dataset: Par[StageOp, Dataset[T]]
                                ): programAlg.FS[stageAlg.FS[Dataset[T]]] =
    if (typeOf[T] == typeOf[Row])
      programAlg
        .persistDataFrame(schema, tableName, dataset.asInstanceOf[Par[StageOp, DataFrame]], writerSettings)
        .asInstanceOf[Par[ProgramOp, Par[StageOp, Dataset[T]]]]
    else {
      programAlg.persistDataset[T](schema, tableName, dataset, implicits.selectEncoder(ExpressionEncoder()), strict = false, writerSettings)
    }
}