package enki
package program

import cats._
import cats.data._
import cats.implicits._
import freestyle.free.FreeS._
import freestyle.free._
import org.apache.spark.sql._

final class StageOpProvider[StageOp[_]] {
  @free abstract class ProgramAlg {
    def persistDataFrame(
                          schemaName: String,
                          tableName: String,
                          dataFrame: Par[StageOp, DataFrame],
                          writerSettings: Par[StageOp, WriterSettings]
                        ): FS[Par[StageOp, DataFrame]]

    def persistDataset[T](
                           schemaName: String,
                           tableName: String,
                           dataset: Par[StageOp, Dataset[T]],
                           encoder: Encoder[T],
                           strict: Boolean,
                           writerSettings: Par[StageOp, WriterSettings]
                         ): FS[Par[StageOp, Dataset[T]]]
  }

  class ProgramSplitter(implicit stager: SparkAlg[StageOp]) extends ProgramAlg.Handler[StageWriter[StageOp, ?]] {
    override protected[this] def persistDataFrame(
                                                   schemaName: String,
                                                   tableName: String,
                                                   dataframe: Par[StageOp, DataFrame],
                                                   writerSettings: Par[StageOp, WriterSettings]
                                                 ): StageWriter[StageOp, Par[StageOp, DataFrame]] = {
      val stageName = s"$schemaName.$tableName"
      val stage = stager.writeDataFrame(schemaName, tableName) <*> writerSettings <*> dataframe
      for {
        _ <- Writer.tell[List[(String, Par[StageOp, _])]](List((stageName, stage)))
      } yield {
        stager.readDataFrame(schemaName, tableName)
      }
    }

    override protected[this] def persistDataset[T](
                                                    schemaName: String,
                                                    tableName: String,
                                                    dataset: Par[StageOp, Dataset[T]],
                                                    encoder: Encoder[T],
                                                    strict: Boolean,
                                                    writerSettings: Par[StageOp, WriterSettings]
                                                  ): StageWriter[StageOp, Par[StageOp, Dataset[T]]] = {
      val stageName = s"$schemaName.$tableName"
      val stage = stager.writeDataset[T](schemaName, tableName, encoder, strict) <*> writerSettings <*> dataset
      for {
        _ <- Writer.tell[List[(String, Par[StageOp, _])]](List((stageName, stage)))
      } yield {
        stager.readDataset[T](schemaName, tableName, encoder, strict)
      }
    }
  }
}