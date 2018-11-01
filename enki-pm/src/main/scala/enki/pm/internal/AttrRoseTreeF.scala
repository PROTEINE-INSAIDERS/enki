package enki.pm.internal

import qq.droste.data._

object AttrRoseTreeF {
  def unapply[A, B, C](arg: AttrRoseTreeF[A, B, C]): Option[(A, RoseTreeF[B, C])] = AttrF.unapply(arg)
}
