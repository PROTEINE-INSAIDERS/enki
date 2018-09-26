package enki

import cats._
import cats.data._
import cats.free.Free._
import cats.free._
import cats.implicits._
import freestyle.free._
import org.apache.spark.sql._
import scalax.collection.Graph
import scalax.collection.GraphPredef._

import scala.collection.mutable

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
                                           stage: FreeS.Par[Stage.Op, DataFrame],
                                           writerSettings: FreeS.Par[DataFrameWriter.Op, Unit]
                                         ) extends ProgramAction[FreeS.Par[Stage.Op, DataFrame]]

  final case class PersistDatasetAction[T](
                                            schemaName: String,
                                            tableName: String,
                                            stage: FreeS.Par[Stage.Op, Dataset[T]],
                                            encoder: Encoder[T],
                                            strict: Boolean,
                                            writerSettings: FreeS.Par[DataFrameWriter.Op, Unit]
                                          ) extends ProgramAction[FreeS.Par[Stage.Op, Dataset[T]]]

  type Program[A] = Free[ProgramAction, A]

  def emptyProgram: Program[FreeS.Par[Stage.Op, Unit]] = ??? // pure(emptyStage)

  def persistDataFrame(
                        schemaName: String,
                        tableName: String,
                        stage: FreeS.Par[Stage.Op, DataFrame],
                        writerSettings: FreeS.Par[DataFrameWriter.Op, Unit]
                      ): Program[FreeS.Par[Stage.Op, DataFrame]] =
    liftF[ProgramAction, FreeS.Par[Stage.Op, DataFrame]](PersistDataFrameAction(
      schemaName,
      tableName,
      stage,
      writerSettings))

  def persistDataset[T](
                         schemaName: String,
                         tableName: String,
                         stage: FreeS.Par[Stage.Op, Dataset[T]],
                         encoder: Encoder[T],
                         strict: Boolean,
                         writerSettings: FreeS.Par[DataFrameWriter.Op, Unit]
                       ): Program[FreeS.Par[Stage.Op, Dataset[T]]] =
    liftF[ProgramAction, FreeS.Par[Stage.Op, Dataset[T]]](PersistDatasetAction[T](
      schemaName,
      tableName,
      stage,
      encoder,
      strict,
      writerSettings))

  type StageWriter[A] = Writer[List[(String, FreeS.Par[Stage.Op, _])], A]

  val programSplitter: ProgramAction ~> StageWriter = λ[ProgramAction ~> StageWriter] {
    /*
    case p: PersistDatasetAction[t] => {
      val stageName = s"${p.schemaName}.${p.tableName}"
      val stage = p.stage ap writeDataset[t](p.schemaName, p.tableName, p.encoder, p.strict, p.writerSettings)
      for {
        _ <- Writer.tell[List[(String, FreeS.Par[Stage.Op, _])]](List((stageName, stage)))
      } yield {
        readDataset[t](p.schemaName, p.tableName, p.encoder, p.strict)
      }
    }
    case p: PersistDataFrameAction => {
      val stageName = s"${p.schemaName}.${p.tableName}"
      val stage = p.stage ap writeDataFrame(p.schemaName, p.tableName, p.writerSettings)
      for {
        _ <- Writer.tell[List[(String, FreeS.Par[Stage.Op, _])]](List((stageName, stage)))
      } yield {
        readDataFrame(p.schemaName, p.tableName)
      }
    }
    */
    case _ => ???
  }

  def buildActionGraph[T](rootName: String, p: Program[FreeS.Par[Stage.Op, _]]): ActionGraph = {
    ???
    /*
    val (stages, lastStage) = p.foldMap(programSplitter).run

    val allStages = ((rootName, lastStage) :: stages).filter { case (_, stage) => stageNonEmpty(stage) } //TODO: стейджи, не содержащие write action попадают в граф, что, возможно, не верно.
    val createdIn = mutable.Map[String, String]()
    allStages.foreach { case (stageName, stage) =>
      stageWrites(stage, Set(_)).foreach { w =>
        val qualifiedName = s"${w.schemaName}.${w.tableName}"
        createdIn.get(qualifiedName) match {
          case None => createdIn += qualifiedName -> stageName
          case Some(stageName1) =>
            //TODO: Сейчас расстановка зависимостей выполняется в зависимости от имён генерируемых таблиц.
            // это не единственный вариант, и не самый верный с точки зрения представления программы в виде
            // монады. Такой подход следует использовать в случае direct sql execution, но для программы
            // следует полагаться на явную вставку зависимостей programSplitter-ом (добавить явные засисимостияв stage?)
            throw new Exception(s"Table $qualifiedName written both in $stageName1 and $stageName.")
        }
      }
    }
    allStages
      .foldMap { case (name, stage) =>
        //TODO: разрешение зависимостей будет встроено в API графа.
        val dependencies = stageReads(stage, Set(_)).flatMap { r => createdIn.get(s"${r.schemaName}.${r.tableName}") }
        if (dependencies.isEmpty)
          ActionGraph(name, stage)
        else
          ActionGraph(Graph(dependencies.toSeq.map(name ~> _): _*), Map(name -> StageNode(stage)))
      }
      */
  }
}
