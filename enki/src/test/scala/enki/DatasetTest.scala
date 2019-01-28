package enki

import enki.default._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._

import scala.beans.BeanProperty

class DatasetTest extends EnkiTestSuite {
  import sparkSession.implicits._

  "typedCol" should {
    "resolve column by field accessor" in {
      val ds = sparkSession.emptyDataset[(Int, String)]

      import ds.sparkSession.implicits._

      val col: TypedColumn[(Int, String), Int] = ds.typedCol(_._1)
      col.expr match {
        case named: NamedExpression => named.name shouldBe "_1"
      }
    }

    "resolve Option to underlying type" in {
      val ds = sparkSession.emptyDataset[(Int, Option[String])]
      "val col: TypedColumn[(Int, Option[String]), String] = ds.typedCol(_._2)" should compile
    }

    "support symbolic alias" in {
      val ds = sparkSession.emptyDataset[(Int, String)]
      "val col = ds $ (_._1)" should compile
    }

    "support java beans" in {
      class Bean {
        @BeanProperty var field1: Int = 0
        @BeanProperty var field2: Boolean = false
      }
      implicit val beanEncoder: Encoder[Bean] = Encoders.bean(classOf[Bean])
      val ds = sparkSession.emptyDataset[Bean]
      val col: TypedColumn[Bean, Int] = ds.typedCol(_.field1)
      col.expr match {
        case named: NamedExpression => named.name shouldBe "field1"
      }
    }
  }

  "columnName" in {
    val ds = sparkSession.emptyDataset[(Int, String)]
    val columnName: String = ds.colName(_._1)
    columnName shouldBe "_1"
  }
}
