package enki
package program

import cats.data.Writer
import freestyle.free.FreeS.Par

trait Module {
  type StageOpProvider[StageOp[_]] = program.StageOpProvider[StageOp]

  // Writer monad for program stages abstracted over stage operation.
  type StageWriter[StageOp[_], A] = Writer[List[(String, Par[StageOp, _])], A]
}