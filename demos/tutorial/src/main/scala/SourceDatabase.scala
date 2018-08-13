import java.sql.Timestamp

import cats.free.FreeApplicative._
import cats.implicits._
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
                     @decimalPrecision(19, 4) price: BigDecimal
                   )

object SourceDatabase extends Database {
  override def schema: String = "source_db"

  val clients: Stage[Dataset[Client]] = read[Client]("client")
  val products: Stage[Dataset[Product]] = read[Product]("product")
  val purchases: Stage[Dataset[Purchase]] = read[Purchase]("purchase", strict = true)

  def createDemoTables(session: SparkSession): Unit = {
    session.sql(s"create database $schema")

    val createClients: Stage[Unit] = dataset(Seq(
      Client(id = 1, name = "Vasily Chapayev"),
      Client(id = 2, name = "Pyotr Pustota")
    )) ap write("client")

    val createProducts: Stage[Unit] = dataset(Seq(
      Product(id = 1, name = "Buddha's Little Finger"),
      Product(id = 2, name = "Dharmachakra")
    )) ap write("product")

    val createPurchases: Stage[Unit] = dataset(Seq(
      Purchase(id = 1, client_id = 1, product_id = 1, date = timestamp"1918-01-28", 108),
      Purchase(id = 2, client_id = 2, product_id = 2, date = timestamp"1919-09-05", 8)
    ), strict = true, allowTruncate = true) ap write("purchase", strict = true)

    (createClients *> createProducts *> createPurchases).foldMap(stageCompiler).apply(session)

    session.sql(s"show create table $schema.purchase").show(false)
  }
}
