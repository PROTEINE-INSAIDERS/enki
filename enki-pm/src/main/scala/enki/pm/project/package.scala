package enki.pm

import qq.droste.data.{AttrF, CoattrF}

package object project {
  /**
    * Rose tree
    * @tparam Leaf leaf type
    * @tparam A fixpoint type
    */
  type RoseTreeF[Leaf, A] = CoattrF[List, Leaf, A]

  /**
    * Rose tree with attribute attached to each node.
    * @tparam Leaf leaf type
    * @tparam Attr attribute type
    * @tparam A fixpoint type
    */
  type AttrRoseTreeF[Leaf, Attr, A] = AttrF[RoseTreeF[Leaf, ?], Attr, A]
}
