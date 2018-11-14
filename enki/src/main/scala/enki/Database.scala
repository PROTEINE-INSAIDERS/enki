package enki

import cats.implicits._
import enki.spark.ReaderSettings
import freestyle.free.FreeS._
import freestyle.free.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe._

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

  protected def readerSettings(tableName: String): Par[StageOp, ReaderSettings] =
    tableMapper map { f => ReaderSettings(f(TableIdentifier(tableName, Some(schema)))) }

  protected def writerSettings(tableName: String): Par[StageOp, WriterSettings] =
    tableMapper map { f => WriterSettings(f(TableIdentifier(tableName, Some(schema)))) }

  protected def planTransformer: Par[StageOp, PlanTransformer] =
    tableMapper map { f =>
      plan =>
        plan.transform {
          case UnresolvedRelation(identifier) => UnresolvedRelation(f(identifier))
        }
    }

  protected def tableMapper: Par[StageOp, TableIdentifier => TableIdentifier] = (identity[TableIdentifier] _).pure[Par[StageOp, ?]]

  /* syntactic sugar */

  /* stages */

  final def arg1(name: String)(implicit alg: SparkAlg[StageOp]): alg.FS[String] = alg.arg(name)

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
    alg.readDataFrame(schema, tableName) <*> readerSettings(tableName)

  final def read[T: Encoder](tableName: String, strict: Boolean = false)(implicit alg: SparkAlg[StageOp]): alg.FS[Dataset[T]] =
    alg.readDataset(schema, tableName, implicitly[Encoder[T]], strict) <*> readerSettings(tableName)

  final def gread[T: TypeTag](tableName: String)(implicit alg: SparkAlg[StageOp]): alg.FS[Dataset[T]] =
    if (typeOf[T] == typeOf[Row]) {
      (alg.readDataFrame(schema, tableName) <*> readerSettings(tableName)).asInstanceOf[alg.FS[Dataset[T]]]
    } else {
      alg.readDataset[T](schema, tableName, implicits.selectEncoder(ExpressionEncoder()), strict = false) <*> readerSettings(tableName)
    }

  //TODO: тут можно использовать planTransformer для добавления параметров партиционирования.
  final def sql(sqlSting: String)
               (implicit alg: SparkAlg[StageOp]): alg.FS[DataFrame] = alg.sql(sqlSting) <*> planTransformer

  final def write(
                   tableName: String
                 )
                 (
                   implicit alg: SparkAlg[StageOp]
                 ): alg.FS[DataFrame => Unit] = write(tableName, writerSettings(tableName))

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
    alg.writeDataset[T](schema, tableName, implicitly, strict) <*> writerSettings(tableName)
  }

  final def gwrite[T: TypeTag](tableName: String)(implicit alg: SparkAlg[StageOp]): alg.FS[Dataset[T] => Unit] =
    if (typeOf[T] == typeOf[Row]) {
      (alg.writeDataFrame(schema, tableName) <*> writerSettings(tableName)).asInstanceOf[alg.FS[Dataset[T] => Unit]]
    } else {
      alg.writeDataset[T](schema, tableName, implicits.selectEncoder(ExpressionEncoder()), strict = false) <*> writerSettings(tableName)
    }

  /* program builder */

  final def persist[T: Encoder](
                                 tableName: String,
                                 dataset: Par[StageOp, Dataset[T]],
                                 strict: Boolean = false
                               )(implicit alg: StageOpProvider[StageOp]#ProgramAlg[ProgramOp]): alg.FS[Par[StageOp, Dataset[T]]] =
    alg.persistDataset(schema, tableName, dataset, implicitly, strict, readerSettings(tableName), writerSettings(tableName))

  final def persist(
                     tableName: String,
                     dataframe: Par[StageOp, DataFrame]
                   )
                   (
                     implicit alg: StageOpProvider[StageOp]#ProgramAlg[ProgramOp]
                   ): alg.FS[Par[StageOp, DataFrame]] = persistPart(tableName, dataframe, Seq.empty[(String, String)].pure[Par[StageOp, ?]])

  final def persistPart(
                         tableName: String,
                         dataframe: Par[StageOp, DataFrame],
                         partition: Par[StageOp, Seq[(String, String)]]
                       )
                       (
                         implicit alg: StageOpProvider[StageOp]#ProgramAlg[ProgramOp]
                       ): alg.FS[Par[StageOp, DataFrame]] = {
    val rws = (partition, readerSettings(tableName), writerSettings(tableName)) mapN { (p: Seq[(String, String)], rs: ReaderSettings, ws: WriterSettings) =>
      if (p.isEmpty) {
        (rs, ws)
      } else {
        (rs.setPartition(p: _*), ws.setPartition(p: _*))
      }
    }
    alg.persistDataFrame(schema, tableName, dataframe, rws.map(_._1), rws.map(_._2))
  }


  final def gpersist[T: TypeTag](
                                  tableName: String,
                                  dataset: Par[StageOp, Dataset[T]]
                                )(implicit alg: StageOpProvider[StageOp]#ProgramAlg[ProgramOp]): alg.FS[Par[StageOp, Dataset[T]]] =
    if (typeOf[T] == typeOf[Row])
      alg
        .persistDataFrame(schema, tableName, dataset.asInstanceOf[Par[StageOp, DataFrame]], readerSettings(tableName), writerSettings(tableName))
        .asInstanceOf[Par[ProgramOp, Par[StageOp, Dataset[T]]]]
    else {
      alg.persistDataset[T](schema, tableName, dataset, implicits.selectEncoder(ExpressionEncoder()), strict = false, readerSettings(tableName), writerSettings(tableName))
    }

  final def run(
                 stageName: String,
                 stage: Par[StageOp, Unit]
               )
               (
                 implicit alg: StageOpProvider[StageOp]#ProgramAlg[ProgramOp]
               ): alg.FS[Par[StageOp, Unit]] =
    alg.run(stageName, stage)
}