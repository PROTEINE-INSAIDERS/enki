package enki
package spark

trait Exports {
  type SparkAlg[F[_]] = spark.SparkAlg[F]
  val SparkAlg: spark.SparkAlg.type = spark.SparkAlg

  type SparkHandler = spark.SparkHandler

  type ReadTableAction = spark.ReadTableAction

  type ReadDatasetAction[A] = spark.ReadDatasetAction[A]

  type WriteTableAction = spark.WriteTableAction

  type WriterSettings = spark.WriterSettings
  val WriterSettings: spark.WriterSettings.type = spark.WriterSettings

  type TableNameMapper = spark.TableNameMapper
}
