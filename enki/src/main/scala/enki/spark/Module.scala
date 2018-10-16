package enki
package spark

trait Module {
  type SparkAlg[F[_]] = spark.SparkAlg[F]
  val SparkAlg: spark.SparkAlg.type = spark.SparkAlg

  type SparkHandler[M[_]] = spark.SparkHandler[M]

  type ReadTableAction = spark.ReadTableAction

  type ReadDatasetAction[A] = spark.ReadDatasetAction[A]

  type TableReads[M] = spark.TableReads[M]
  val TableReads: spark.TableReads.type = spark.TableReads

  type TableWrites[M] = spark.TableWrites[M]

  type WriteTableAction = spark.WriteTableAction

  type WriterSettings = spark.WriterSettings
  val WriterSettings: spark.WriterSettings.type = spark.WriterSettings

  type TableNameMapper = spark.TableMapper
  val TableNameMapper: spark.TableMapper.type = spark.TableMapper
}
