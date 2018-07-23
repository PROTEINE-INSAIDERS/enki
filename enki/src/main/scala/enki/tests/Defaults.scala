package enki.tests

import java.sql.Timestamp

import shapeless._

trait DefaultsModule {

  trait Default[A] {
    def default: A
  }

  implicit val defaultBigDecimal: Default[BigDecimal] = new Default[BigDecimal] {
    override def default: BigDecimal = 0
  }

  implicit val defaultBoolean: Default[Boolean] = new Default[Boolean] {
    override def default: Boolean = false
  }

  implicit val defaultInt: Default[Int] = new Default[Int] {
    override def default: Int = 0
  }

  implicit val defaultLong: Default[Long] = new Default[Long] {
    override def default: Long = 0
  }

  implicit val defaultString: Default[String] = new Default[String] {
    override def default: String = ""
  }

  implicit val defaultTimestamp: Default[Timestamp] = new Default[Timestamp] {
    override def default: Timestamp = new Timestamp(0)
  }


  implicit def defaultOption[T]: Default[Option[T]] = new Default[Option[T]] {
    override def default: Option[T] = None
  }


  implicit def defaultGeneric[T, L <: HList](implicit g: Generic.Aux[T, L], d: Default[L]): Default[T] =
    new Default[T] {
      override def default: T = g.from(d.default)
    }

  implicit val defaultHNil: Default[HNil] = new Default[HNil] {
    override def default: HNil = HNil
  }

  implicit def defaultHCons[H, T <: HList](implicit hd: Default[H], td: Default[T]): Default[H :: T] =
    new Default[H :: T] {
      override def default: H :: T = hd.default :: td.default
    }


  def default[T: Default](implicit d: Default[T]): T = d.default
}
