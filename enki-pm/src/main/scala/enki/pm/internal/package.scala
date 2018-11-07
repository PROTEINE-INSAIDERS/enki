package enki.pm

import cats._
import cats.data._
import cats.implicits._
import enki.pm.internal.Validated._
import qq.droste.CoalgebraM
import qq.droste.data._

package object internal {
  /**
    * Rose tree
    *
    * @tparam A leaf type.
    */
  type RoseTree[A] = Coattr[List, A]

  /**
    * The pattern functor for [[RoseTree]]
    *
    * @tparam A leaf type
    * @tparam A fix type
    */
  type RoseTreeF[A, B] = CoattrF[List, A, B]

  type AttrRoseTree[A, B] = Attr[RoseTreeF[B, ?], A]

  /**
    * Rose tree with attribute attached to each node.
    *
    * @tparam A attribute type
    * @tparam B leaf type
    * @tparam C fixpoint type
    */
  type AttrRoseTreeF[A, B, C] = AttrF[RoseTreeF[B, ?], A, C]

  type ValidationErrorItem = String

  type ValidationErrorContainer[A] = NonEmptyChain[A]

  type ValidationError = ValidationErrorContainer[ValidationErrorItem]

  /**
    * Unified Validated type for enki package manager.
    *
    * @tparam A Validated value.
    */
  type Validated[A] = cats.data.Validated[ValidationError, A]

  def all: Monoid[Boolean] = new Monoid[Boolean] {
    override def empty: Boolean = true

    override def combine(x: Boolean, y: Boolean): Boolean = x && y
  }
}
