package enki

trait StageModule {

  import cats._
  import cats.free.FreeApplicative._
  import cats.free._
  import cats.implicits._
  import org.apache.spark.sql._
  import org.apache.spark.sql.catalyst.encoders._

  import scala.reflect.runtime.universe.{TypeTag, typeOf}

  sealed trait StageA[A]

  final case class ReadAction[T](f: SparkSession => Dataset[T]) extends StageA[Dataset[T]]

  final case class WriteAction[T](f: Dataset[T] => Unit) extends StageA[Dataset[T] => Unit]

  type Stage[A] = FreeApplicative[StageA, A]

  type DataStage[T] = Stage[Dataset[T]]

  def emptyStage: Stage[Unit] = ().pure[Stage]

  def read[T](f: SparkSession => Dataset[T]): Stage[Dataset[T]] = lift[StageA, Dataset[T]](ReadAction[T](f))

  def read[T: TypeTag](tableName: String)(implicit database: Database): Stage[Dataset[T]] = {
    val reader: SparkSession => Dataset[T] = session => {
      if (typeOf[T] == typeOf[Row])
        database.readTable(session, tableName).asInstanceOf[Dataset[T]]
      else
        database.readTable(session, tableName).as[T](ExpressionEncoder())
    }

    this.read(reader)
  }

  def write[T](f: Dataset[T] => Unit): Stage[Dataset[T] => Unit] = lift[StageA, Dataset[T] => Unit](WriteAction[T](f))

  type FromSession[A] = SparkSession => A

  val stageCompiler: StageA ~> FromSession = λ[StageA ~> FromSession] {
    case ReadAction(f) => session => f(session)
    case WriteAction(f) => session => f
  }

  def readActions(stage: Stage[_]): Seq[ReadAction[_]] = {
    type ActionM = List[ReadAction[_]]

    stage.analyze(λ[StageA ~> λ[α => ActionM]] {
      case r: ReadAction[_] => List(r)
      case _ => Monoid.empty[ActionM]
    })
  }
}