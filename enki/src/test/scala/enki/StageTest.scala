package enki

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

// @decimalPrecision(10, 5)
case class DecimalPrecisionTestData( a: Option[BigDecimal], b: Option[String])

class StageTest extends EnkiTestSuite {
  "ReadAction" should {
    "handle non-default decimal's scale and precision in strict mode" in {

      val schema = StructType(Array(StructField(name = "a", dataType = DecimalType(10, 5)), StructField(name = "b", dataType = StringType)))
      sparkSession.sqlContext.createDataFrame(Seq(Row(BigDecimal(1999), "test")), schema).write.saveAsTable("default.DecimalPrecisionTestData")

      sparkSession.sqlContext.table("default.DecimalPrecisionTestData").schema.foreach(println)


      val ra = new ReadAction[DecimalPrecisionTestData](schemaName = "default", tableName = "DecimalPrecisionTestData", strict = false)
      ra(sparkSession).schema.foreach(println)
      println(ra(sparkSession).collect())
    }
  }

}
