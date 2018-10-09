package enki
package spark

trait Module {
  type SparkAlg[F[_]] = spark.SparkAlg[F]
  val SparkAlg: spark.SparkAlg.type = spark.SparkAlg

  type SparkHandler[M[_]] = spark.SparkHandler[M]

  type ReadTableAction = spark.ReadTableAction

  type ReadDatasetAction[A] = spark.ReadDatasetAction[A]

  type WriteTableAction = spark.WriteTableAction

  type WriterSettings = spark.WriterSettings
  val WriterSettings: spark.WriterSettings.type = spark.WriterSettings

  type TableNameMapper = spark.TableNameMapper
  val TableNameMapper: spark.TableNameMapper.type = spark.TableNameMapper
}
