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
  //TODO: возможно read и write можно поднять на уровень монадки, а вместо Stage использовать Applicative вида Session -> Dataset.
  // тогда все трансформаци программы будут производится путём её исполнения, результатом будут исполняемые stage, внутрь которых
  // заглядывать не надо.
  // При таком подходе граф надо будет строить при интерпретации программы. Пока не ясно, как это делать, единственный вариант -
  // встраивать какую-то дополнительную информацию в Stage (это вернёт нас к необходимости анализировать Stage)

  final case class ReadAction[T](f: SparkSession => Dataset[T]) extends StageA[Dataset[T]]

  final case class WriteAction[T](f: Dataset[T] => Unit, tableName: TableName) extends StageA[Dataset[T] => Unit]

  type Stage[A] = FreeApplicative[StageA, A]

  type DataStage[T] = Stage[Dataset[T]] //TODO: похоже это ломает mapM, надо проверить

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

  def write[T](f: Dataset[T] => Unit, tableName: TableName): Stage[Dataset[T] => Unit] = lift[StageA, Dataset[T] => Unit](WriteAction[T](f, tableName))

  type FromSession[A] = SparkSession => A

  val stageCompiler: StageA ~> FromSession = λ[StageA ~> FromSession] {
    case ReadAction(f) => session => f(session)
    case WriteAction(f, _) => session => f
  }

  def readActions(stage: Stage[_]): Seq[ReadAction[_]] = {
    type ActionM = List[ReadAction[_]]

    stage.analyze(λ[StageA ~> λ[α => ActionM]] {
      case r: ReadAction[_] => List(r)
      case _ => Monoid.empty[ActionM]
    })
  }

  def writeActions(stage: Stage[_]): Seq[WriteAction[_]] = {
    type ActionM = List[WriteAction[_]]

    stage.analyze(λ[StageA ~> λ[α => ActionM]] {
      case r: WriteAction[_] => List(r)
      case _ => Monoid.empty[ActionM]
    })
  }
}