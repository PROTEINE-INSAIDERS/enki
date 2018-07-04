package enki.staging

import enki.tests.EnkiSuite

case class TestData1(a: Int, b: Int)

case class TestData2(a: Int, b: String)

trait FinalProgramBuilder extends EnkiSuite {

  import cats.arrow._
  import cats.free.Free._
  import cats.free.FreeApplicative._
  import cats.free._
  import cats.implicits._
  import org.apache.spark.sql._

  sealed trait StageOp[A]

  final case class Read[T](f: SparkSession => Dataset[T]) extends StageOp[Dataset[T]]

  type Stage[A] = FreeApplicative[StageOp, A]

  def read[T](f: SparkSession => Dataset[T]): Stage[Dataset[T]] = lift[StageOp, Dataset[T]](Read[T](f))

  type FromSession[A] = SparkSession => A

  val stageCompiler = new FunctionK[StageOp, FromSession] {
    override def apply[A](fa: StageOp[A]): FromSession[A] = session =>
      fa match {
        case Read(f) => f(session)
      }
  }

  sealed trait ProgramOp[A]

  final case class Register[T](id: Symbol, stage: Stage[Dataset[T]]) extends ProgramOp[Stage[Dataset[T]]]

  type Program[A] = Free[ProgramOp, A]

  def register[T](id: Symbol)(stage: Stage[Dataset[T]]): Program[Stage[Dataset[T]]] =
    liftF[ProgramOp, Stage[Dataset[T]]](Register[T](id, stage))

  def test(): Unit = {
    import sparkSession.implicits._

    val s1 = read(_.createDataset(Seq(TestData1(10, 19))))
    val s2 = read(_.createDataset(Seq(TestData2(19, "source 2"))))

    def f(d1: Dataset[TestData1], d2: Dataset[TestData2]): DataFrame = {
      d1.join(d2, d1("b") === d2("a"))
    }

    val ss1 = (s1, s2) mapN f

    val compiledStage = ss1.foldMap(stageCompiler)
    compiledStage(sparkSession).show

    val program: Program[Stage[DataFrame]] = for {
      a1 <- register('ss1)(ss1)
      a2 <- register('ss2) {
        (a1, read(_.createDataset(Seq(TestData2(19, "source 3"))))) mapN { (k1, k2) => k1.join(k2, k1("b") === k2("a")) }
      }
    } yield a2
  }
}

object test extends FinalProgramBuilder {

  def main(args: Array[String]): Unit = {
    this.test()
  }
}