import javassist.bytecode.stackmap.TypeTag
import org.apache.spark.sql.{Dataset, SparkSession}

package object enki
  extends AllModules {
  type SparkAction[A] = SparkSession => A
}