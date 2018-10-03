package enki

import cats._
import freestyle.free._
import freestyle.free.implicits._
import freestyle.free.internal.EffectLike
import iota._

/**
  * Instantiated Enki module parametrized by operation types.
  */
trait Enki
  extends enki.Exports
    with DataFrameModule
    with enki.stage.Aliases
    with enki.application.ApplicationModule
    with enki.args.Aliases
    with enki.ds.Extensions
    with enki.GraphModule
    with enki.program.ActionGraphBuilder
    with enki.stage.Analyzers {
  type StageOp[A]
  type ProgramOp[A]

  val stageAlg: EffectLike[StageOp]
  val programAlg: EffectLike[ProgramOp]

  type Stage[A] = stageAlg.FS[A]
  type Program[A] = programAlg.FS[A]
  type ProgramS[A] = FreeS[ProgramOp, A]
  type StageCompiler = FSHandler[StageOp, EnkiMonad]

  /* implicits */
  implicit val programSplitter: FSHandler[ProgramOp, StageWriter[StageOp, ?]] // сплиттер возможно тоже не должен быть имплицитным.
  val stageCompiler: StageCompiler // не должен быть имплицитным, т.к. начинает конфликтовать с имплицитами из freestyle.free.implicits._

  def analyzeArgs[M: Monoid](s: Stage[_], f: ArgsAlg.Op ~> λ[α => M]): M

  def analyzeStages[M: Monoid](s: Stage[_], f: StageAlg.Op ~> λ[α => M]): M
}

/**
  * Default stage operations.
  */
@module trait StagesWithArgs {
  val stage: StageAlg
  val args: ArgsAlg
}

/**
  * Default enki instance (import enki.default._ for basic functionality)
  */
object default extends Enki {

  val programWrapper = new ProgramWrapper[StageOp]

  override type StageOp[A] = StagesWithArgs.Op[A]
  override type ProgramOp[A] = programWrapper.ProgramM.Op[A]

  override val stageAlg: enki.StagesWithArgs[StageOp] = implicitly
  override val programAlg: enki.Program1[StageOp, ProgramOp] = implicitly

  implicit val defaultStageCompiler: FSHandler[StageAlg.Op, EnkiMonad] = new enki.stage.DefaultStageCompiler {}
  implicit val defaultArgsCompiler: FSHandler[ArgsAlg.Op, EnkiMonad] = new ArgsCompiler {}
  override implicit val programSplitter: FSHandler[ProgramOp, StageWriter[StageOp, ?]] = new programWrapper.ProgramSplitter()

  override val stageCompiler: StageCompiler = implicitly

  trait Database extends enki.Database[ProgramOp, StageOp] {
    override val programAlg: enki.Program1[StageOp, ProgramOp] = implicitly
    override val stageAlg: enki.StageAlg[StageOp] = implicitly
    override val argsAlg: enki.ArgsAlg[StageOp] = implicitly
  }

  override def analyzeArgs[M: Monoid](s: Stage[_], f: ArgsAlg.Op ~> λ[α => M]): M = {
    val I = CopK.Inject[ArgsAlg.Op, StageOp]
    s.analyze(λ[StageOp ~> λ[α => M]] {
      case I(a) => f(a)
      case _ => Monoid.empty[M]
    })
  }

  override def analyzeStages[M: Monoid](s: Stage[_], f: StageAlg.Op ~> λ[α => M]): M = {
    val I = CopK.Inject[StageAlg.Op, StageOp]
    s.analyze(λ[StageOp ~> λ[α => M]] {
      case I(a) => f(a)
      case _ => Monoid.empty[M]
    })
  }
}