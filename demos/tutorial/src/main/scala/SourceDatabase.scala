import java.sql.Timestamp

import enki._
import enki.timestamp._
import org.apache.spark.sql._

case class Client(
                   id: Long,
                   name: String
                 )

case class Product(
                    id: Long,
                    name: String
                  )

case class Purchase(
                     id: Long,
                     client_id: Long,
                     product_id: Long,
                     date: Timestamp,
                     price: BigDecimal
                   )

object SourceDatabase extends Database {
  override def schema: String = "source_db"

  val clients: Stage[Dataset[Client]] = read[Client]("client")
  val products: Stage[Dataset[Product]] = read[Product]("product")
  val purchases: Stage[Dataset[Purchase]] = read[Purchase]("purchase")

  def createDemoTables(session: SparkSession): Unit = {
    import session.implicits._

    session.sql(s"create database $schema")

    val clients = session.createDataset(Seq(
      Client(id = 1, name = "Vasily Chapayev"),
      Client(id = 2, name = "Pyotr Pustota")
    ))

    val products = session.createDataset(Seq(
      Product(id = 1, name = "Buddha's Little Finger"),
      Product(id = 2, name = "Dharmachakra")
    ))

    val purchases = session.createDataset(Seq(
      Purchase(id = 1, client_id = 1, product_id = 1, date = timestamp"1918-01-28", 108),
      Purchase(id = 2, client_id = 2, product_id = 2, date = timestamp"1919-09-05", 8)
    ))

    clients.write.saveAsTable(s"$schema.client")
    products.write.saveAsTable(s"$schema.product")
    purchases.write.saveAsTable(s"$schema.purchase")
  }
}
