package enki

import cats._
import cats.data._
import cats.mtl.ApplicativeAsk
import enki.internal._
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
    with enki.spark.Module
    with enki.application.Module
    with enki.arg.Module
    with enki.GraphModule
    with enki.program.ActionGraphBuilder
    with enki.spark.Analyzers
    with enki.spark.sql.Module {
  type StageOp[A]
  type ProgramOp[A]

  val stageAlg: EffectLike[StageOp]
  val programAlg: EffectLike[ProgramOp]

  type Stage[A] = stageAlg.FS[A]
  type Program[A] = programAlg.FS[A]
  type ProgramS[A] = FreeS[ProgramOp, A]
  type StageHandler = FSHandler[StageOp, EnkiMonad]

  /* implicits */
  implicit val programSplitter: FSHandler[ProgramOp, StageWriter[StageOp, ?]] // сплиттер возможно тоже не должен быть имплицитным.
  val stageHandler: StageHandler // не должен быть имплицитным, т.к. начинает конфликтовать с имплицитами из freestyle.free.implicits._

  def analyzeArgs[M: Monoid](s: Stage[_], f: ArgAlg.Op ~> λ[α => M]): M

  def analyzeStages[M: Monoid](s: Stage[_], f: SparkAlg.Op ~> λ[α => M]): M
}

/**
  * Default stage operations.
  */
@module trait StagesWithArgs {
  val sparkAlg: SparkAlg
  val argsAlg: enki.arg.ArgAlg
}

/**
  * Default enki instance (import enki.default._ for basic functionality)
  */
object default extends Enki {
  val programWrapper = new StageOpProvider[StageOp]

  override type StageOp[A] = StagesWithArgs.Op[A]
  override type ProgramOp[A] = programWrapper.ProgramAlg.Op[A]

  override type ArgOp[A] = StagesWithArgs.Op[A]

  override val stageAlg: enki.StagesWithArgs[StageOp] = implicitly
  override val programAlg: programWrapper.ProgramAlg[ProgramOp] = implicitly

  implicit val sparkHandler: FSHandler[SparkAlg.Op, EnkiMonad] = new enki.spark.SparkHandler {}

  implicit val argsReader: ApplicativeAsk[EnkiMonad, Parameters] = new ApplicativeAsk[EnkiMonad, Parameters] {
    override val applicative: Applicative[default.EnkiMonad] = implicitly

    override def ask: default.EnkiMonad[default.Parameters] = Reader { env => Parameters(env.parameters) }

    override def reader[A](f: default.Parameters => A): default.EnkiMonad[A] = Reader { env => f(Parameters(env.parameters)) }
  }

  implicit val argsHandler: FSHandler[ArgAlg.Op, EnkiMonad] = new ArgHandler[EnkiMonad]

  override implicit val programSplitter: FSHandler[ProgramOp, StageWriter[StageOp, ?]] = new programWrapper.ProgramSplitter()

  override val stageHandler: StageHandler = implicitly

  override val injectArg: ArgAlg.Op :<: StageOp = CopK.Inject[ArgAlg.Op, StageOp]

  trait Database extends enki.Database {
    override type ProgramOp[A] = default.this.ProgramOp[A]
    override type StageOp[A] = default.this.StageOp[A]

    override val enki = default
  }

  def test[M: Monoid](s: Stage[_], f: ArgAlg.Op ~> λ[α => M]): Unit = {
    implicit val in = CopK.Inject[ArgAlg.Op, StageOp]
    val k = s.analyzeIn(f)

  }

  override def analyzeArgs[M: Monoid](s: Stage[_], f: ArgAlg.Op ~> λ[α => M]): M = {
    val I = CopK.Inject[ArgAlg.Op, StageOp]
    s.analyze(λ[StageOp ~> λ[α => M]] {
      case I(a) => f(a)
      case _ => Monoid.empty[M]
    })
  }

  override def analyzeStages[M: Monoid](s: Stage[_], f: SparkAlg.Op ~> λ[α => M]): M = {
    val I = CopK.Inject[SparkAlg.Op, StageOp]
    s.analyze(λ[StageOp ~> λ[α => M]] {
      case I(a) => f(a)
      case _ => Monoid.empty[M]
    })
  }
}