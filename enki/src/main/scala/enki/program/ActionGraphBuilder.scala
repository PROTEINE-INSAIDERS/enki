package enki
package program

import cats.implicits._
import enki.actiontree._
import freestyle.free.FreeS._
import freestyle.free._
import qq.droste.data.Fix
import scalax.collection.Graph
import scalax.collection.GraphPredef._
import enki.internal._

import scala.collection._

trait ActionGraphBuilder {
  self: Enki with GraphModule =>

  import implicits._

  // сплиттер нужно передавать как параметр, т.к. программа может содержать кастомные шаги.
  def buildActionGraph(
                        rootName: String,
                        p: ProgramS[Stage[Unit]]
                      ): ActionGraph = {
    val (stages, lastStage) = p.interpret[StageWriter[StageOp, ?]].run // foldMap(programSplitter).run

    val allStages = ((rootName, lastStage) :: stages).filter { case (_, stage) => stageNonEmpty(stage) } //TODO: стейджи, не содержащие write action попадают в граф, что, возможно, не верно.
    val createdIn = mutable.Map[String, String]()

    val writesAnalyzer = new TableWrites[immutable.Set[WriteTableAction]](immutable.Set(_)).analyzer

    allStages.foreach { case (stageName, stage) =>
      stage.analyzeIn(writesAnalyzer).foreach { w =>
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

    val readsAnalyzer = new TableReads[immutable.Set[ReadTableAction]](immutable.Set(_)).analyzer

    allStages
      .foldMap { case (name, stage) =>
        //TODO: разрешение зависимостей будет встроено в API графа.
        val dependencies = stage.analyzeIn(readsAnalyzer).flatMap { r => createdIn.get(s"${r.schemaName}.${r.tableName}") }
        if (dependencies.isEmpty)
          ActionGraph(name, stage)
        else {
          new ActionGraph(Graph(dependencies.toSeq.map(name ~> _): _*), immutable.Map(name -> StageNode(stage)))
        }
      }
  }
}
