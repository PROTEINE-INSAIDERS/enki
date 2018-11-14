import java.sql.Timestamp

import cats._
import cats.implicits._
import enki.arg.ArgAlg
import enki.default._
import freestyle.free._
import freestyle.free.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe._

case class PurchasesReport(
                            purchase_id: Long,
                            client_id: Long,
                            client_name: String,
                            product_id: Long,
                            product_name: String,
                            date: Timestamp,
                            @decimalPrecision(precision = 19, scale = 4) price: BigDecimal
                          )

case class ProductsByClientReport(
                                   client_id: Long,
                                   client_name: String,
                                   product_id: Long,
                                   product_name: String,
                                   @decimalPrecision(precision = 19, scale = 4, allowTruncate = true) total_sum: BigDecimal
                                 )

trait UserDatabase extends Database {
  self: SourceDatabase =>

  import implicits._

  override def schema: String = "user_db"

  // Changing encoder style to Enki to enable annotation processing.
  override def encoderStyle: EncoderStyle = EncoderStyle.Enki

  override def writerSettings(tableName: String): Stage[WriterSettings] =
    (
      super.writerSettings(tableName),
      arg("overwrite", "Overwrite existent data.", defaultValue = Some(true))
    ) mapN { (settings, overwrite) =>
      settings
    }

  def purchasesReport(clients: Dataset[Client],
                      products: Dataset[Product],
                      purchases: Dataset[Purchase]): Dataset[PurchasesReport] =
    purchases
      .join(broadcast(products), purchases.$(_.product_id) === products.$(_.id))
      .join(broadcast(clients), purchases.$(_.client_id) === clients.$(_.id))
      .select(
        purchases $ (_.id) as "purchase_id",
        clients $ (_.id) as "client_id",
        clients $ (_.name) as "client_name",
        products $ (_.id) as "product_id",
        products $ (_.name) as "product_name",
        purchases $ (_.date) as "date",
        purchases $ (_.price) as "price"
      ).as[PurchasesReport]

  def productByClientReport(purchasesReport: Dataset[PurchasesReport]): Dataset[ProductsByClientReport] =
    purchasesReport
      .groupBy(
        purchasesReport $ (_.client_id) as "client_id",
        purchasesReport $ (_.product_id) as "product_id")
      .agg(
        first(purchasesReport $ (_.client_name)) as "client_name",
        first(purchasesReport $ (_.product_name)) as "product_name",
        sum(purchasesReport $ (_.price)) as "total_sum"
      ).as[ProductsByClientReport]

  def persistPurchasesReport: Program[Stage[Dataset[PurchasesReport]]] =
    persist[PurchasesReport]("purchases_report", (clients, products, purchases) mapN this.purchasesReport)

  def customAction(purchasesReport: Dataset[PurchasesReport], productsByClientReport: Dataset[ProductsByClientReport]) = {
    val a = purchasesReport.count()
    val b = productsByClientReport.count()
    println(s"Total count: ${a + b}")
  }

  def createReports: ProgramS[Stage[Unit]] = for {
    purchasesReport <- persistPurchasesReport
    productsByClientReport <- persist(
      "products_by_client_report",
      purchasesReport map this.productByClientReport)
    _ <- run("custom action",  (purchasesReport, productsByClientReport) mapN customAction)
  } yield ().pure[Stage]

  def createDatabase(session: SparkSession): Unit = {
    session.sql(s"create database $schema")
    ()
  }
}