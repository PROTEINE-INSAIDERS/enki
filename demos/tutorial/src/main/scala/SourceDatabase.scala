import java.sql.Timestamp

import enki.StagesWithArgs.Op
import enki.default._
import freestyle.free.FreeS.Par
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
                     @decimalPrecision(precision = 19, scale = 4) price: BigDecimal
                   )

trait SourceDatabase extends Database {

  import implicits._

  override def schema: String = "source_db"

  // Changing encoder style to Enki to enable annotation processing.
  override def encoderStyle: EncoderStyle = EncoderStyle.Enki

  def clients: Stage[Dataset[Client]] = read[Client]("client")

  def products: Stage[Dataset[Product]] = read[Product]("product")

  def purchases: Stage[Dataset[Purchase]] = read[Purchase]("purchase", strict = true)
}
