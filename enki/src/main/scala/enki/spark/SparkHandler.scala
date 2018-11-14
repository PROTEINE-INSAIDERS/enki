package enki
package spark

import cats.mtl._
import enki.spark.plan.PlanAnalyzer
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

class SparkHandler[M[_]](implicit env: ApplicativeAsk[M, SparkSession], planAnalyzer: PlanAnalyzer) extends SparkAlg.Handler[M] {
  private[enki] def write[T](
                              session: SparkSession,
                              writerSettings: WriterSettings,
                              dataset: Dataset[T]
                            ): Unit = {
    val tableName = writerSettings.tableIdentifier.table
    val schemaName = writerSettings.tableIdentifier.database.getOrElse("default")
    (session.catalog.tableExists(schemaName, tableName), writerSettings.partition) match {
      case (true, partition) if partition.nonEmpty =>
        dataset
          .where(partition map (p => dataset(p._1) === lit(p._2)) reduce (_ and _))
          .drop(partition.map(_._1): _*)
          .createTempView(s"tmp_$tableName")
        try {
          val partitionStr = partition.map(a => s"${a._1} = '${a._2}'").mkString(", ")
          session.sql(s"insert ${if (writerSettings.overwrite) "overwrite" else "into"} table $schemaName.$tableName partition($partitionStr) select * from tmp_$tableName")
          ()
        } finally {
          session.catalog.dropTempView(s"tmp_$tableName")
          ()
        }
      case (false, partition) if partition.nonEmpty =>
        writerSettings.configure(dataset.write) //TODO: filter records by partition.
          .partitionBy(partition.map(_._1): _*)
          .saveAsTable(s"$schemaName.$tableName")
      case _ =>
        val writer = writerSettings.configure(dataset.write)
        writer.saveAsTable(s"$schemaName.$tableName")
    }
  }

  override protected[this] def arg(name: String): M[String] = env.reader { session =>
    session.sessionState.conf.getConfString(name)
  }

  override protected[this] def dataFrame(
                                          rows: Seq[Row],
                                          schema: StructType
                                        ): M[DataFrame] = env.reader { session =>
    session.createDataFrame(rows, schema)
  }


  override protected[this] def dataset[T](
                                           data: Seq[T],
                                           encoder: Encoder[T]
                                         ): M[Dataset[T]] = env.reader { session =>
    session.createDataset[T](data)(encoder)
  }

  override protected[this] def readDataFrame(
                                              schemaName: String,
                                              tableName: String
                                            ): M[ReaderSettings => DataFrame] = env.reader { session =>
    readerSettings => session.table(s"${readerSettings.tableIdentifier.database.getOrElse("default")}.${readerSettings.tableIdentifier.table}")
  }

  override protected[this] def readDataset[T](
                                               schemaName: String,
                                               tableName: String,
                                               encoder: Encoder[T],
                                               strict: Boolean
                                             ): M[ReaderSettings => Dataset[T]] = env.reader { session =>
    readerSettings =>
      val dataframe = session.table(s"${readerSettings.tableIdentifier.database.getOrElse("default")}.${readerSettings.tableIdentifier.table}")
      val restricted = if (strict) {
        dataframe.select(encoder.schema.map(f => dataframe(f.name)): _*)
      } else {
        dataframe
      }
      restricted.as[T](encoder)
  }

  override protected[this] def plan(logicalPlan: LogicalPlan): M[PlanTransformer => DataFrame] = env.reader { session =>
    transformer =>
      val qe = session.sessionState.executePlan(transformer(logicalPlan))
      qe.assertAnalyzed()
      new Dataset[Row](session, qe.logical, RowEncoder(qe.analyzed.schema))
  }

  override protected[this] def session: M[SparkSession] = env.ask

  override protected[this] def sql(sqlText: String): M[PlanTransformer => DataFrame] = env.reader { session =>
    transformer =>
      val logicalPlan = session.sessionState.sqlParser.parsePlan(sqlText)
      val qe = session.sessionState.executePlan(transformer(logicalPlan))
      qe.assertAnalyzed()
      new Dataset[Row](session, qe.logical, RowEncoder(qe.analyzed.schema))
  }

  override protected[this] def writeDataFrame(
                                               schemaName: String,
                                               tableName: String
                                             ): M[WriterSettings => DataFrame => Unit] = env.reader { session =>
    writerSettings => dataFrame => write[Row](session, writerSettings, dataFrame)
  }

  override protected[this] def writeDataset[T](
                                                schemaName: String,
                                                tableName: String,
                                                encoder: Encoder[T],
                                                strict: Boolean
                                              ): M[WriterSettings => Dataset[T] => Unit] = env.reader { session =>
    writerSettings =>
      dataset =>
        if (strict) {
          write(
            session,
            writerSettings,
            dataset.select(encoder.schema.map(f => dataset(f.name)): _*).as[T](encoder))
        } else {
          write(session, writerSettings, dataset)
        }
  }
}
