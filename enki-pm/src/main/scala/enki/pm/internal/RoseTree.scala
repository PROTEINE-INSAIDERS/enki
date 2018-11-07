package enki.pm.internal

import qq.droste.data._

object RoseTree {
  def leaf[A](a: A): RoseTree[A] = Coattr.pure(a)

  def node[A](s: List[RoseTree[A]]): RoseTree[A] = Coattr.roll(s)

  object Node {
    def unapply[A](arg: RoseTree[A]): Option[List[RoseTree[A]]] = Coattr.un(arg).right.toOption
  }
}

object RoseTreeF {
  def un[A, B](arg: RoseTreeF[A, B]): Either[A, List[B]] = CoattrF.un(arg)

  def node[A, B](subtrees: List[B]): RoseTreeF[A, B] = CoattrF.roll(subtrees)

  def leaf[A, B](a: A): RoseTreeF[A, B] = CoattrF.pure(a)

  object Leaf {
    def unapply[A](arg: RoseTreeF[A, _]): Option[A] = CoattrF.un(arg).left.toOption
  }
  object Node {
    def unapply[A, B](arg: RoseTreeF[A, B]): Option[List[B]] = CoattrF.un(arg).right.toOption
  }
}
