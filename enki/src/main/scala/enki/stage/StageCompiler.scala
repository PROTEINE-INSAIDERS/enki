package enki
package stage

import cats.data._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

trait StageCompiler extends Stage.Handler[Reader[Environment, ?]] {
  private[enki] def write[T](
                              schemaName: String,
                              tableName: String,
                              writerSettings: WriterSettings[T],
                              dataset: Dataset[T]
                            ): Unit = {
    val session = dataset.sparkSession
    (session.catalog.tableExists(schemaName, tableName), writerSettings.partition) match {
      case (true, partition) if partition.nonEmpty =>
        dataset
          .where(partition map (p => dataset(p._1) === lit(p._2)) reduce (_ and _))
          .drop(partition.map(_._1): _*)
          .createTempView(s"tmp_$tableName")
        try {
          val partitionStr = partition.map(a => s"${a._1} = '${a._2}'").mkString(", ")
          session.sql(s"insert ${if (writerSettings.overwrite) "overwrite" else ""} table $schemaName.$tableName partition($partitionStr) select * from tmp_$tableName")
          ()
        } finally {
          session.catalog.dropTempView(s"tmp_$tableName")
          ()
        }
      case (false, partition) if partition.nonEmpty =>
        writerSettings.configure(dataset.write) //TODO: filter records by partition.
          .partitionBy(partition.map(_._1): _*)
          .saveAsTable(s"$schemaName.$tableName")
      case _ => writerSettings.configure(dataset.write).saveAsTable(s"$schemaName.$tableName")
    }
  }

  override protected[this] def dataFrame(
                                          rows: Seq[Row],
                                          schema: StructType
                                        ): Reader[Environment, DataFrame] = Reader { env =>
    env.session.createDataFrame(rows, schema)
  }


  override protected[this] def dataset[T](
                                           data: Seq[T],
                                           encoder: Encoder[T]
                                         ): Reader[Environment, Dataset[T]] = Reader { env =>
    env.session.createDataset[T](data)(encoder)
  }

  override protected[this] def readDataFrame(
                                              schemaName: String,
                                              tableName: String
                                            ): Reader[Environment, DataFrame] = Reader { env =>
    env.session.table(s"$schemaName.$tableName")
  }

  override protected[this] def readDataset[T](
                                               schemaName: String,
                                               tableName: String,
                                               encoder: Encoder[T],
                                               strict: Boolean
                                             ): Reader[Environment, Dataset[T]] = Reader { env =>
    val dataframe = env.session.table(s"$schemaName.$tableName")
    val restricted = if (strict) {
      dataframe.select(encoder.schema.map(f => dataframe(f.name)): _*)
    } else {
      dataframe
    }
    restricted.as[T](encoder)
  }


  override protected[this] def writeDataFrame(
                                               schemaName: String,
                                               tableName: String
                                             ): Reader[Environment, WriterSettings[Row] => DataFrame => Unit] = Reader { env =>
    writerSettings => dataFrame => write[Row](schemaName, tableName, writerSettings, dataFrame)
  }

  override protected[this] def writeDataset[T](
                                                schemaName: String,
                                                tableName: String,
                                                encoder: Encoder[T],
                                                strict: Boolean
                                              ): Reader[Environment, WriterSettings[T] => Dataset[T] => Unit] = Reader { env =>
    writerSettings =>
      dataset =>
        if (strict) {
          write(
            schemaName,
            tableName,
            writerSettings,
            dataset.select(encoder.schema.map(f => dataset(f.name)): _*).as[T](encoder))
        } else {
          write(schemaName, tableName, writerSettings, dataset)
        }
  }
}
