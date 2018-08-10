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

  //TODO: тут нужен ридер и райтер. их может предоставить база данных.
  final case class PersistAction[T: TypeTag](
                                              schemaName: String,
                                              tableName: String,
                                              stage: enki.Stage[Dataset[T]],
                                              strict: Boolean,
                                              saveMode: Option[SaveMode]
                                            ) extends ProgramAction[Stage[Dataset[T]]] {
    private[ProgramModule] def tag: TypeTag[T] = implicitly[TypeTag[T]]
  }

  type Program[A] = Free[ProgramAction, A]

  def persist[T: TypeTag](
                           schemaName: String,
                           tableName: String,
                           stage: Stage[Dataset[T]],
                           strict: Boolean,
                           saveMode: Option[SaveMode]
                         ): Program[Stage[Dataset[T]]] =
    liftF[ProgramAction, Stage[Dataset[T]]](PersistAction[T](
      schemaName,
      tableName,
      stage,
      strict,
      saveMode))

  type StageWriter[A] = Writer[List[(String, Stage[_])], A]

  val programSplitter: ProgramAction ~> StageWriter = λ[ProgramAction ~> StageWriter] {
    case p: PersistAction[t] => {
      val stageName = s"${p.schemaName}.${p.tableName}"
      val stage = p.stage ap write[t](p.schemaName, p.tableName, p.strict, p.saveMode)(p.tag)
      for {
        _ <- Writer.tell[List[(String, Stage[_])]](List((stageName, stage)))
      } yield {
        read[t](p.schemaName, p.tableName, p.strict)(p.tag)
      }
    }
  }

  def buildActionGraph[T](rootName: String, p: Program[Stage[T]]): ActionGraph = {
    val (stages, lastStage) = p.foldMap(programSplitter).run

    val allStages = ((rootName, lastStage) :: stages).filter { case (_, stage) => stageNonEmpty(stage) }

    val createdIn = allStages.flatMap { case (name, stage) =>
      stageWrites(stage).map(w => (s"${w.schemaName}.${w.tableName}", name))
    }.toMap

    allStages
      .foldMap { case (name, stage) =>
        val dependencies = stageReads(stage).flatMap { r => createdIn.get(s"${r.schemaName}.${r.tableName}") }
        if (dependencies.isEmpty)
          ActionGraph(Graph(name), Map(name -> stage))
        else
          ActionGraph(Graph(dependencies.toSeq.map(name ~> _): _*), Map(name -> stage))
      }
  }
}
