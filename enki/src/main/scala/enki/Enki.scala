package enki

import cats._
import cats.data._
import cats.mtl._
import freestyle.free.FreeS._
import freestyle.free._
import freestyle.free.implicits._
import iota._
import org.apache.spark.sql._

trait EnkiTypes {
  // Enki arguments

  type ProgramOp[A]
  type StageOp[A]
  type StageMonad[A] //not necessary monad

  // Type aliases (parameters depended).

  type Stage[A] = Par[StageOp, A]
  type Program[A] = Par[ProgramOp, A]
  type ProgramS[A] = FreeS[ProgramOp, A]
  type StageHandler = FSHandler[StageOp, StageMonad]
}

/**
  * Instantiated Enki module parametrized by operation types.
  */
trait Enki extends EnkiTypes
  with enki.Exports
  with DataFrameModule
  with enki.spark.Module
  with enki.application.Module
  with enki.arg.Module
  with enki.GraphModule
  with enki.program.ActionGraphBuilder
  with enki.spark.Analyzers
  with enki.spark.sql.Module
  with enki.spark.plan.Module {

  /* implicits */

  implicit val programSplitter: FSHandler[ProgramOp, StageWriter[StageOp, ?]] // сплиттер возможно тоже не должен быть имплицитным.

  implicit val planAnalyzer: PlanAnalyzer = new PlanAnalyzer {}

  val stageHandler: StageHandler // не должен быть имплицитным, т.к. начинает конфликтовать с имплицитами из freestyle.free.implicits._

  //TODO: remove (use analyzeIn)
  def analyzeStages[M: Monoid](s: Stage[_], f: SparkAlg.Op ~> λ[α => M]): M

  //TODO: remove
  def run(stage: StageMonad[_], env: Environment): Unit
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


  // override Enki parameters

  override type ProgramOp[A] = programWrapper.ProgramAlg.Op[A]
  override type StageOp[A] = StagesWithArgs.Op[A]
  override type StageMonad[A] = Reader[Environment, A]

  // set module parameters

  override val stageApplicative: Applicative[Reader[Environment, ?]] = implicitly
  override val injectSpark: SparkAlg.Op :<: StageOp = CopK.Inject[SparkAlg.Op, StageOp]

  override type ArgOp[A] = StageOp[A]
  override val injectArg: ArgAlg.Op :<: StageOp = CopK.Inject[ArgAlg.Op, StageOp]

  // stage handlers

  implicit val askSession: ApplicativeAsk[StageMonad, SparkSession] = new DefaultApplicativeAsk[StageMonad, SparkSession] {
    override val applicative: Applicative[StageMonad] = implicitly

    override def ask: StageMonad[SparkSession] = Reader(_.session)
  }

  implicit val sparkHandler: FSHandler[SparkAlg.Op, StageMonad] = new enki.spark.SparkHandler {}

  implicit val askArgs: ApplicativeAsk[StageMonad, Parameters] = new DefaultApplicativeAsk[StageMonad, Parameters] {
    override val applicative: Applicative[StageMonad] = implicitly

    override def ask: StageMonad[Parameters] = Reader { env => Parameters(env.parameters) }
  }

  implicit val argsHandler: FSHandler[ArgAlg.Op, StageMonad] = new ArgHandler[StageMonad]

  override val stageHandler: StageHandler = implicitly

  // program handlers

  override implicit val programSplitter: FSHandler[ProgramOp, StageWriter[StageOp, ?]] = new programWrapper.ProgramSplitter()

  // parametrized classes

  trait Database extends enki.Database {
    override type ProgramOp[A] = default.this.ProgramOp[A]
    override type StageOp[A] = default.this.StageOp[A]

    override val enki = default
  }

  //TODO: remove
  override def analyzeStages[M: Monoid](s: Stage[_], f: SparkAlg.Op ~> λ[α => M]): M = {
    val I = CopK.Inject[SparkAlg.Op, StageOp]
    s.analyze(λ[StageOp ~> λ[α => M]] {
      case I(a) => f(a)
      case _ => Monoid.empty[M]
    })
  }

  //TODO: remove
  override def run(stage: StageMonad[_], env: Environment) = {
    stage.run(env)
  }
}