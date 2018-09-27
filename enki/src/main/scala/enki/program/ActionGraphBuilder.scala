package enki
package program

import freestyle.free.FreeS._
import freestyle.free._

trait ActionGraphBuilder {
  self: Enki =>
  // сплиттер нужно передавать как параметр, т.к. программа может содержать кастомные шаги.
  def buildActionGraph(
                        rootName: String,
                        p: FreeS[ProgramOp, Par[StageOp, Unit]]
                      ): ActionGraph = {
    // implicit val splitter: FSHandler[ProgramAlg, StageWriter] = new ProgramSplitter[ProgramAlg]()


    //  val int = p.interpret[Id]
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
