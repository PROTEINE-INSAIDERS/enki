package enki.pm

import iota._
import iota._
import TList.::
import TListK.:::

object IotaTest {
  type Foo = Cop[Int :: String :: Double :: TNil]
  type Bar = Prod[Int :: String :: Double :: TNil]

}
