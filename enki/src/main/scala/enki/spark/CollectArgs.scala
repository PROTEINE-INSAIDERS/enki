package enki.spark

import cats._
import cats.data._
import freestyle.free._

case class Argument(name: String)

case class CollectArgs[M: Monoid](f: Argument => M) extends FSHandler[SparkAlg.Op, Const[M, ?]] {
  private val REF_RE = "\\$\\{(?:(\\w+?):)?(\\S+?)\\}".r

  override def apply[A](fa: SparkAlg.Op[A]): Const[M, A] = Const {
    fa match {
      case SparkAlg.ArgOp(name) => f(Argument(name))
      case SparkAlg.SqlOp(sqlText) => Monoid.combineAll(
        REF_RE.findAllMatchIn(sqlText).map { m =>
          val prefix = m.group(1)
          val name = m.group(2)
          val ref = if (prefix == null) name else s"$prefix:$name"
          f(Argument(ref))
        })
      case _ => Monoid.empty[M]
    }
  }
}