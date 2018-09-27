package enki
package program

import cats.data.Writer
import freestyle.free.FreeS.Par

trait Aliases {
  type ProgramWrapper[StageOp[_]] = program.ProgramWrapper[StageOp]
  type Program1[StageOp[_], ProgramOp[_]] = program.ProgramWrapper[StageOp]#ProgramM[ProgramOp]

  // Writer monad for program stages abstracted over stage operations.
  type StageWriter[StageOp[_], A] = Writer[List[(String, Par[StageOp, _])], A]
}