package enki

import cats._
import cats.arrow.FunctionK
import cats.data.{Writer, _}
import cats.implicits._
import enki.Combined.Op
import freestyle.free.FreeS.Par
import freestyle.free._
import iota.CopK

@free trait Alg1 {
  def m1(str: String): FS[String]
}

trait Alg2[FF$147[_]] extends _root_.freestyle.free.internal.EffectLike[FF$147] {
  def m2(): FS[String => Unit]
}

@_root_.java.lang.SuppressWarnings(_root_.scala.Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Throw"))
object Alg2 {

  sealed trait Op[_] extends _root_.scala.Product with _root_.java.io.Serializable {
    val FSAlgebraIndex148: _root_.scala.Int
  }

  final case class M2Op() extends _root_.scala.AnyRef with Op[String => Unit] {
    override val FSAlgebraIndex148: _root_.scala.Int = 0
  }

  type OpTypes = _root_.iota.TConsK[Op, _root_.iota.TNilK]

  trait Handler[MM$152[_]] extends _root_.freestyle.free.FSHandler[Op, MM$152] {
    protected[this] def m2(): MM$152[String => Unit]

    override def apply[AA$153](fa$154: Op[AA$153]): MM$152[AA$153] = ((fa$154.FSAlgebraIndex148: @_root_.scala.annotation.switch) match {
      case 0 =>
        val fresh155: M2Op = fa$154.asInstanceOf[M2Op]
        m2()
      case i =>
        throw new _root_.java.lang.Exception("freestyle internal error: index " + i.toString() + " out of bounds for " + this.toString())
    }).asInstanceOf[MM$152[AA$153]]
  }

  class To[LL$149[_]](implicit ii$150: _root_.freestyle.free.InjK[Op, LL$149]) extends Alg2[LL$149] {
    private[this] val toInj151 = _root_.freestyle.free.FreeS.inject[Op, LL$149](ii$150)

    override def m2(): FS[String => Unit] = toInj151(M2Op())
  }

  implicit def to[LL$149[_]](implicit ii$150: _root_.freestyle.free.InjK[Op, LL$149]): To[LL$149] = new To[LL$149]

  def apply[FF$147[_]](implicit ev$156: Alg2[FF$147]): Alg2[FF$147] = ev$156

  def instance(implicit ev: Alg2[Op]): Alg2[Op] = ev
}

trait Combined[FF$311[_]] extends _root_.freestyle.free.internal.EffectLike[FF$311] {
  val a1: Alg1[FF$311]
  val a2: Alg2[FF$311]
}

object Combined {
  type OpTypes = _root_.iota.TListK.Op.Concat[Alg1.OpTypes, Alg2.OpTypes]
  type Op[AA$68] = _root_.iota.CopK[OpTypes, AA$68]

  class To[GG$67[_]](implicit val a1: Alg1[GG$67], val a2: Alg2[GG$67]) extends Combined[GG$67] {}

  implicit def to[GG$67[_]](implicit a1: Alg1[GG$67], a2: Alg2[GG$67]): To[GG$67] = new To[GG$67]()

  def apply[FF$311[_]](implicit ev$69: Combined[FF$311]): Combined[FF$311] = ev$69

  def instance(implicit ev: Combined[Op]): Combined[Op] = ev
}

object ComposableAlg {
  implicit val h1: Alg1.Handler[Id] = new Alg1.Handler[Id] {
    override protected[this] def m1(str: String): Id[String] = str
  }

  implicit val h2: Alg2.Handler[Id] = new Alg2.Handler[Id] {
    override protected[this] def m2(): Id[String => Unit] = println(_)
  }

  def p1[F[_]](implicit a1: Alg1[F], a2: Alg2[F]): Par[F, Unit] = {
    import a1._
    import a2._

    m1("test").ap(m2())
  }

  def main(args: Array[String]): Unit = {


    val A1 = CopK.Inject[Alg1.Op, Combined.Op]

    val p = p1[Combined.Op].analyze(λ[Combined.Op ~> λ[α => List[String]]] {
      case A1(a) =>
        println(a)
        List.empty[String]
      case _ =>
        List.empty
    })
    println(p)
    // interpret[Const[Unit, ?]]
  }
}
