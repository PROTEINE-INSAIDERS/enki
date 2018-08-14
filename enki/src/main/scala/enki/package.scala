import org.apache.spark.sql._

package object enki
  extends AllModules {
  type SparkAction[A] = SparkSession => A

  /**
    * Since using SparkImplicits and SparkSession.implicits at once will lead to ambiguity, SparkImplicits not imported by default.
    */
  val implicits: SparkImplicits = new SparkImplicits {}
}