package enki.pm

import cats._
import qq.droste.data._

package object internal {
  /**
    * Rose tree
    *
    * @tparam A leaf type
    * @tparam A fix type
    */
  type RoseTreeF[A, B] = CoattrF[List, A, B]

  /**
    * Rose tree with attribute attached to each node.
    *
    * @tparam Leaf leaf type
    * @tparam Attr attribute type
    * @tparam A    fixpoint type
    */
  type AttrRoseTreeF[Attr, Leaf, A] = AttrF[RoseTreeF[Leaf, ?], Attr, A]

  /**
    * Unified Validated type for enki package manager.
    *
    * @tparam A Validated value.
    */
  type Validated[A] = cats.data.ValidatedNel[String, A]

  def all = new Monoid[Boolean] {
    override def empty: Boolean = true

    override def combine(x: Boolean, y: Boolean): Boolean = x && y
  }
}
