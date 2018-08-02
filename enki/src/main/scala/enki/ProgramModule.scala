package enki

import cats._
import cats.data._
import cats.free.Free._
import cats.free._
import cats.implicits._
import org.apache.spark.sql._
import scalax.collection.Graph
import scalax.collection.GraphPredef._

import scala.reflect.runtime.universe.TypeTag

trait ProgramModule {
  this: GraphModule =>

  sealed trait ProgramAction[A]

  final case class PersistAction[T: TypeTag](tableName: String,
                                             stage: Stage[Dataset[T]],
                                             database: Database) extends ProgramAction[Stage[Dataset[T]]] {
    private[ProgramModule] def tag: TypeTag[T] = implicitly[TypeTag[T]]
  }

  type Program[A] = Free[ProgramAction, A]

  def persist[T: TypeTag](database: Database, tableName: String, stage: Stage[Dataset[T]]): Program[Stage[Dataset[T]]] =
    liftF[ProgramAction, Stage[Dataset[T]]](PersistAction[T](
      tableName,
      stage,
      database))

  type StageWriter[A] = Writer[List[(String, Stage[_])], A]

  val programSplitter: ProgramAction ~> StageWriter = λ[ProgramAction ~> StageWriter] {
    case p: PersistAction[t] => {
      //TODO: надо использовать стабильные имена.
      //при этом во многих случаях имя таблицы вполне подходит в качестве стабильного имени,
      //просто должен быть механизм перегрузки этого имени в момент чтения/записи на базе конфигурации.
      val stageName = s"${p.database.schema}.${p.tableName}"
      val stage = p.stage ap write[t](p.database, p.tableName)
      for {
        _ <- Writer.tell[List[(String, Stage[_])]](List((stageName, stage)))
      } yield {
        read[t](p.database, p.tableName, false, stageName)(p.tag)
      }
    }
  }

  def buildActionGraph[T](rootName: String, p: Program[Stage[T]]): ActionGraph = {
    val (stages, lastStage) = p.foldMap(programSplitter).run
    ((rootName, lastStage) :: stages) foldMap { case (name, stage) => ActionGraph(name, stage) }
  }
}
