package enki
package program

import cats.data._
import cats.implicits._
import enki.spark.ReaderSettings
import freestyle.free.FreeS._
import freestyle.free._
import org.apache.spark.sql._

final class StageOpProvider[StageOp[_]] {

  @free abstract class ProgramAlg {
    def persistDataFrame(
                          schemaName: String,
                          tableName: String,
                          dataFrame: Par[StageOp, DataFrame],
                          readerSettings: Par[StageOp, ReaderSettings],
                          writerSettings: Par[StageOp, WriterSettings]
                        ): FS[Par[StageOp, DataFrame]]

    def persistDataset[T](
                           schemaName: String,
                           tableName: String,
                           dataset: Par[StageOp, Dataset[T]],
                           encoder: Encoder[T],
                           strict: Boolean,
                           readerSettings: Par[StageOp, ReaderSettings],
                           writerSettings: Par[StageOp, WriterSettings]
                         ): FS[Par[StageOp, Dataset[T]]]

    def run(
             stageName: String,
             stage: Par[StageOp, Unit]
           ): FS[Par[StageOp, Unit]]
  }

  //TODO: для реализации партиционирования можно анализировать WriterSettings и добавлять дополнительные фильтры в ридер.
  // вопрос - насколько это правильно?
  // Как минимум, настройки ридера и writer-а должны быть согласованы между собой.
  class ProgramSplitter(implicit stager: SparkAlg[StageOp]) extends ProgramAlg.Handler[StageWriter[StageOp, ?]] {
    override protected[this] def persistDataFrame(
                                                   schemaName: String,
                                                   tableName: String,
                                                   dataframe: Par[StageOp, DataFrame],
                                                   readerSettings: Par[StageOp, ReaderSettings],
                                                   writerSettings: Par[StageOp, WriterSettings]
                                                 ): StageWriter[StageOp, Par[StageOp, DataFrame]] = {
      val stageName = s"$schemaName.$tableName"
      val stage = stager.writeDataFrame(schemaName, tableName) <*> writerSettings <*> dataframe
      for {
        _ <- Writer.tell[List[(String, Par[StageOp, _])]](List((stageName, stage)))
      } yield {
        stager.readDataFrame(schemaName, tableName) <*> readerSettings
      }
    }

    override protected[this] def persistDataset[T](
                                                    schemaName: String,
                                                    tableName: String,
                                                    dataset: Par[StageOp, Dataset[T]],
                                                    encoder: Encoder[T],
                                                    strict: Boolean,
                                                    readerSettings: Par[StageOp, ReaderSettings],
                                                    writerSettings: Par[StageOp, WriterSettings]
                                                  ): StageWriter[StageOp, Par[StageOp, Dataset[T]]] = {
      val stageName = s"$schemaName.$tableName"
      val stage = stager.writeDataset[T](schemaName, tableName, encoder, strict) <*> writerSettings <*> dataset
      for {
        _ <- Writer.tell[List[(String, Par[StageOp, _])]](List((stageName, stage)))
      } yield {
        stager.readDataset[T](schemaName, tableName, encoder, strict) <*> readerSettings
      }
    }

    override protected[this] def run(
                                      stageName: String,
                                      stage: Par[StageOp, Unit]
                                    ): StageWriter[StageOp, Par[StageOp, Unit]] = {
      for {
        _ <- Writer.tell[List[(String, Par[StageOp, _])]](List((stageName, stage)))
      } yield {
        ().pure[Par[StageOp, ?]]
      }
    }
  }

}