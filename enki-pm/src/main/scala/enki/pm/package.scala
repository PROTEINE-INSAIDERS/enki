package enki

import qq.droste.data._

package object pm {
  /**
    * Rose tree abstracted over leaf-type A and recursion fixpoint B.
    */
  type RoseTree[A, B] = CoattrF[List, A, B]
}
