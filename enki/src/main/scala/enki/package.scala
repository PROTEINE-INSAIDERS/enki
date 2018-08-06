import org.apache.spark.sql.SparkSession

package object enki
  extends AllModules {
  type SparkAction[A] = SparkSession => A

}