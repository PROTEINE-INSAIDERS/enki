import java.sql.Timestamp

import cats.implicits._
import enki._
import enki.sparkImplicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class PurchasesReport(
                            purchase_id: Long,
                            client_id: Long,
                            client_name: String,
                            product_id: Long,
                            product_name: String,
                            date: Timestamp,
                            @decimalPrecision(19, 4) price: BigDecimal
                          )

case class ProductsByClientReport(
                                   client_id: Long,
                                   client_name: String,
                                   product_id: Long,
                                   product_name: String,
                                   @decimalPrecision(19, 4) total_sum: BigDecimal
                                 )

object UserDatabase extends Database {
  override def schema: String = "user_db"

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

  import SourceDatabase._

  val program: Program[Stage[Unit]] = for {
    purchasesReport <- persist[PurchasesReport](
      "purchases_report",
      (clients, products, purchases) mapN this.purchasesReport)

    _ <- persist(
      "products_by_client_report",
      purchasesReport fmap this.productByClientReport)
  } yield ().pure[Stage]

  def createDatabase(session: SparkSession): Unit = {
    session.sql(s"create database $schema")
  }
}
