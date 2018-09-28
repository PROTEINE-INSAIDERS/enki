package enki

import cats._
import cats.data._
import cats.instances._
import cats.free._
import freestyle.free.FreeS._
import freestyle.free._
import freestyle.free.implicits._
import iota._

/**
  * Instantiated Enki module parametrized by operation types.
  */
trait Enki
  extends enki.Aliases
    with enki.application.ApplicationModule
    with enki.args.Aliases
    with enki.ds.Extensions
    with MetadataModule
    with enki.GraphModule
    with enki.program.ActionGraphBuilder
    with enki.stage.Analyzers {
  type StageOp[A]
  type ProgramOp[A]

  type Stage[A] = Par[StageOp, A]
  type Program[A] = FreeS[ProgramOp, A]
  type StageCompiler = FSHandler[StageOp, Reader[Environment, ?]]

  implicit val programSplitter: FSHandler[ProgramOp, StageWriter[StageOp, ?]]
  implicit val stageCompiler: StageCompiler

  def analyzeArgs[M: Monoid](s: Stage[_], f: Args.Op ~> λ[α => M]): M
  def analyzeStages[M: Monoid](s: Stage[_], f: Stage.Op ~> λ[α => M]): M
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
  implicit val stageOnlyCompiler = new enki.stage.StageCompiler{}
  implicit val argsOnlyCompiler = new ArgsCompiler{}

  override val stageCompiler : StageCompiler = implicitly

  private val programAlg: enki.Program1[StageOp, ProgramOp] = implicitly
  private val stageAlg: enki.Stage[StageOp] = implicitly

  trait Database extends enki.Database[ProgramOp, StageOp] {
    override val programAlg: enki.Program1[StageOp, ProgramOp] = default.this.programAlg
    override val stageAlg: enki.Stage[StageOp] = default.this.stageAlg
  }

  override def analyzeArgs[M: Monoid](s: Stage[_], f: Args.Op ~> λ[α => M]): M = {
    val I = CopK.Inject[Args.Op, StageOp]
    s.analyze(λ[StageOp ~> λ[α => M]] {
      case I(a) => f(a)
      case _ => Monoid.empty[M]
    })
  }

  override def analyzeStages[M: Monoid](s: Stage[_], f: Stage.Op ~> λ[α => M]): M = {
    val I = CopK.Inject[Stage.Op, StageOp]
    s.analyze(λ[StageOp ~> λ[α => M]] {
      case I(a) => f(a)
      case _ => Monoid.empty[M]
    })
  }
}