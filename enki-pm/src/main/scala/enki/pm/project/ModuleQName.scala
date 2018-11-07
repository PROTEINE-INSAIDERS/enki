package enki.pm.project

import cats._
import cats.data._

case class ModuleQName(qName: Chain[String]) extends AnyRef {
  def :+(a: String): ModuleQName = ModuleQName(qName :+ a)
}

object ModuleQName {
  def empty: ModuleQName = ModuleQName(Chain.empty)

  implicit val showModuleQName: Show[ModuleQName] = new Show[ModuleQName] {
    override def show(a: ModuleQName): String = a.qName.uncons match {
      case Some((head, tail)) => tail.foldLeft(head)((acc, a) => s"$acc.$a")
      case None => ""
    }
  }
}