package enki

import freestyle.free.FreeS._
import freestyle.free._

/**
  * Instantiated Enki module parametrized by operation types.
  */
trait Enki
  extends enki.Aliases
    with enki.application.Aliases
    with enki.ds.Extensions
    with MetadataModule
    with enki.program.ActionGraphBuilder {
  type StageOp[A]
  type ProgramOp[A]

  type Database = enki.Database[ProgramOp, StageOp]
  type Stage[A] = Par[StageOp, A]

  implicit val programSplitter: FSHandler[ProgramOp, StageWriter[StageOp, ?]]
}

/**
  * Default stage operations.
  */
@module trait StagesWithArgs {
  val stage: Stage
  val args: Args
}

/**
  * Default enki instance (import enki.default._ for basic functionality)
  */
object default extends Enki {
  val programWrapper = new ProgramWrapper[StageOp]

  override type StageOp[A] = StagesWithArgs.Op[A]
  override type ProgramOp[A] = programWrapper.ProgramM.Op[A]
  override val programSplitter: FSHandler[ProgramOp, StageWriter[StageOp, ?]] = new programWrapper.ProgramSplitter()
}