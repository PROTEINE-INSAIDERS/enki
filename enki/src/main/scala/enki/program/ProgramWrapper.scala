package enki
package program

import cats._
import cats.data._
import cats.implicits._
import freestyle.free.FreeS._
import freestyle.free._
import org.apache.spark.sql._

final class ProgramWrapper[StageAlg[_]] {
  @free abstract class ProgramM {
    def persistDataFrame(
                          schemaName: String,
                          tableName: String,
                          dataFrame: Par[StageAlg, DataFrame],
                          writerSettings: Par[StageAlg, WriterSettings[Row]]
                        ): FS[Par[StageAlg, DataFrame]]

    def persistDataset[T](
                           schemaName: String,
                           tableName: String,
                           dataset: Par[StageAlg, Dataset[T]],
                           encoder: Encoder[T],
                           strict: Boolean,
                           writerSettings: Par[StageAlg, WriterSettings[T]]
                         ): FS[Par[StageAlg, Dataset[T]]]
  }

  class ProgramSplitter(implicit stager: Stage[StageAlg]) extends ProgramM.Handler[StageWriter[StageAlg, ?]] {
    override protected[this] def persistDataFrame(
                                                   schemaName: String,
                                                   tableName: String,
                                                   dataframe: Par[StageAlg, DataFrame],
                                                   writerSettings: Par[StageAlg, WriterSettings[Row]]
                                                 ): StageWriter[StageAlg, Par[StageAlg, DataFrame]] = {
      val stageName = s"$schemaName.$tableName"
      val stage = stager.writeDataFrame(schemaName, tableName) <*> writerSettings <*> dataframe
      for {
        _ <- Writer.tell[List[(String, Par[StageAlg, _])]](List((stageName, stage)))
      } yield {
        stager.readDataFrame(schemaName, tableName)
      }
    }

    override protected[this] def persistDataset[T](
                                                    schemaName: String,
                                                    tableName: String,
                                                    dataset: Par[StageAlg, Dataset[T]],
                                                    encoder: Encoder[T],
                                                    strict: Boolean,
                                                    writerSettings: Par[StageAlg, WriterSettings[T]]
                                                  ): StageWriter[StageAlg, Par[StageAlg, Dataset[T]]] = {
      val stageName = s"$schemaName.$tableName"
      val stage = stager.writeDataset[T](schemaName, tableName, encoder, strict) <*> writerSettings <*> dataset
      for {
        _ <- Writer.tell[List[(String, Par[StageAlg, _])]](List((stageName, stage)))
      } yield {
        stager.readDataset[T](schemaName, tableName, encoder, strict)
      }
    }
  }

}