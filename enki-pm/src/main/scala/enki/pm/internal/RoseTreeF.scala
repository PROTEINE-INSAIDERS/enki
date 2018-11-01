package enki.pm.internal

import qq.droste.data._

object RoseTreeF {
  def un[A, B](arg: RoseTreeF[A, B]): Either[A, List[B]] = CoattrF.un(arg)

  def node[A, B](subtrees: List[B]): RoseTreeF[A, B] = CoattrF.roll(subtrees)

  def leaf[A, B](a: A): RoseTreeF[A, B] = CoattrF.pure(a)

  object Left {
    def unapply[A, B](arg: RoseTreeF[A, B]): Option[A] = CoattrF.un(arg).left.toOption
  }
  object Right {
    def unapply[A, B](arg: RoseTreeF[A, B]): Option[List[B]] = CoattrF.un(arg).right.toOption
  }
}
