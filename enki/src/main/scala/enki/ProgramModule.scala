package enki

import cats._
import cats.data._
import cats.free.Free._
import cats.free._
import cats.implicits._
import org.apache.spark.sql._
import scalax.collection.Graph
import scalax.collection.GraphPredef._

/**
  * Monadic program builder designed to be used with for-comprehensions.
  *
  * Should not be confused with actual program AST because it's sole purpose is to provide syntatic sugar to construct
  * AST using the Scala syntax.
  */
trait ProgramModule {
  this: GraphModule =>

  sealed trait ProgramAction[A]

  final case class PersistDataFrameAction(
                                           schemaName: String,
                                           tableName: String,
                                           stage: enki.Stage[DataFrame],
                                           saveMode: Option[SaveMode]
                                         ) extends ProgramAction[Stage[DataFrame]]

  final case class PersistDatasetAction[T](
                                            schemaName: String,
                                            tableName: String,
                                            stage: enki.Stage[Dataset[T]],
                                            encoder: Encoder[T],
                                            strict: Boolean,
                                            saveMode: Option[SaveMode]
                                          ) extends ProgramAction[Stage[Dataset[T]]]

  type Program[A] = Free[ProgramAction, A]

  def emptyProgram: Program[Stage[Unit]] = pure(emptyStage)

  def persistDataFrame(
                        schemaName: String,
                        tableName: String,
                        stage: enki.Stage[DataFrame],
                        saveMode: Option[SaveMode]
                      ): Program[Stage[DataFrame]] =
    liftF[ProgramAction, Stage[DataFrame]](PersistDataFrameAction(
      schemaName,
      tableName,
      stage,
      saveMode))

  def persistDataset[T](
                         schemaName: String,
                         tableName: String,
                         stage: Stage[Dataset[T]],
                         encoder: Encoder[T],
                         strict: Boolean,
                         saveMode: Option[SaveMode]
                       ): Program[Stage[Dataset[T]]] =
    liftF[ProgramAction, Stage[Dataset[T]]](PersistDatasetAction[T](
      schemaName,
      tableName,
      stage,
      encoder,
      strict,
      saveMode))

  type StageWriter[A] = Writer[List[(String, Stage[_])], A]

  val programSplitter: ProgramAction ~> StageWriter = λ[ProgramAction ~> StageWriter] {
    case p: PersistDatasetAction[t] => {
      val stageName = s"${p.schemaName}.${p.tableName}"
      val stage = p.stage ap writeDataset[t](p.schemaName, p.tableName, p.encoder, p.strict, p.saveMode)
      for {
        _ <- Writer.tell[List[(String, Stage[_])]](List((stageName, stage)))
      } yield {
        readDataset[t](p.schemaName, p.tableName, p.encoder, p.strict)
      }
    }
    case p: PersistDataFrameAction => {
      val stageName = s"${p.schemaName}.${p.tableName}"
      val stage = p.stage ap writeDataFrame(p.schemaName, p.tableName, p.saveMode)
      for {
        _ <- Writer.tell[List[(String, Stage[_])]](List((stageName, stage)))
      } yield {
        readDataFrame(p.schemaName, p.tableName)
      }
    }
  }

  def buildActionGraph[T](rootName: String, p: Program[Stage[T]]): ActionGraph = {
    val (stages, lastStage) = p.foldMap(programSplitter).run

    val allStages = ((rootName, lastStage) :: stages).filter { case (_, stage) => stageNonEmpty(stage) } //TODO: стейджи, не содержащие write action попадают в граф, что, возможно, не верно.

    val createdIn = allStages.flatMap { case (name, stage) =>
      stageWrites(stage, Set(_)).map(w => (s"${w.schemaName}.${w.tableName}", name))
    }.toMap

    allStages
      .foldMap { case (name, stage) =>
        //TODO: разрешение зависимостей будет встроено в API графа.
        val dependencies = stageReads(stage, Set(_)).flatMap { r => createdIn.get(s"${r.schemaName}.${r.tableName}") }
        if (dependencies.isEmpty)
          ActionGraph(name, stage)
        else
          ActionGraph(Graph(dependencies.toSeq.map(name ~> _): _*), Map(name -> StageNode(stage)))
      }
  }
}
