package enki.args

import freestyle.free._

trait Args[FF$8[_]] extends _root_.freestyle.free.internal.EffectLike[FF$8] {
  def string(name: String, description: String = "", defaultValue: Option[String] = None): FS[String]

  def int(name: String, description: String = "", defaultValue: Option[Int] = None): FS[Int]
}

@_root_.java.lang.SuppressWarnings(_root_.scala.Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Throw")) object Args {

  sealed trait Op[_] extends _root_.scala.Product with _root_.java.io.Serializable {
    val FSAlgebraIndex29: _root_.scala.Int
  }

  final case class StringOp(val name: String, val description: String = "", val defaultValue: Option[String] = None) extends _root_.scala.AnyRef with Op[String] {
    override val FSAlgebraIndex29: _root_.scala.Int = 0
  }

  final case class IntOp(val name: String, val description: String = "", val defaultValue: Option[Int] = None) extends _root_.scala.AnyRef with Op[Int] {
    override val FSAlgebraIndex29: _root_.scala.Int = 1
  }

  type OpTypes = _root_.iota.TConsK[Op, _root_.iota.TNilK]

  trait Handler[MM$90[_]] extends _root_.freestyle.free.FSHandler[Op, MM$90] {
    protected[this] def string(name: String, description: String = "", defaultValue: Option[String] = None): MM$90[String]

    protected[this] def int(name: String, description: String = "", defaultValue: Option[Int] = None): MM$90[Int]

    override def apply[AA$93](fa$137: Op[AA$93]): MM$90[AA$93] = ((fa$137.FSAlgebraIndex29: @_root_.scala.annotation.switch) match {
      case 0 =>
        val fresh140: StringOp = fa$137.asInstanceOf[StringOp]
        string(fresh140.name, fresh140.description, fresh140.defaultValue)
      case 1 =>
        val fresh166: IntOp = fa$137.asInstanceOf[IntOp]
        int(fresh166.name, fresh166.description, fresh166.defaultValue)
      case i =>
        throw new _root_.java.lang.Exception("freestyle internal error: index " + i.toString() + " out of bounds for " + this.toString())
    }).asInstanceOf[MM$90[AA$93]]
  }

  class To[LL$81[_]](implicit ii$82: _root_.freestyle.free.InjK[Op, LL$81]) extends Args[LL$81] {
    private[this] val toInj83 = _root_.freestyle.free.FreeS.inject[Op, LL$81](ii$82)

    override def string(name: String, description: String = "", defaultValue: Option[String] = None): FS[String] = toInj83(StringOp(name, description, defaultValue))

    override def int(name: String, description: String = "", defaultValue: Option[Int] = None): FS[Int] = toInj83(IntOp(name, description, defaultValue))
  }

  implicit def to[LL$81[_]](implicit ii$82: _root_.freestyle.free.InjK[Op, LL$81]): To[LL$81] = new To[LL$81]

  def apply[FF$8[_]](implicit ev$168: Args[FF$8]): Args[FF$8] = ev$168

  def instance(implicit ev: Args[Op]): Args[Op] = ev
}
