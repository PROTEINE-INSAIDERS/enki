import java.sql.Timestamp

import enki._
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

trait SourceDatabase[StageAlg[_], ProgramAlg[_]] extends Database[StageAlg, ProgramAlg] {

  import implicits._
  import stage._

  override def schema: String = "source_db"

  // Changing encoder style to Enki to enable annotation processing.
  override def encoderStyle: EncoderStyle = EncoderStyle.Enki

  def clients: FS[Dataset[Client]] = read[Client]("client")

  def products: FS[Dataset[Product]] = read[Product]("product")

  def purchases: FS[Dataset[Purchase]] = read[Purchase]("purchase", strict = true)
}
