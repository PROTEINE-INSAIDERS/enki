package enki

trait ProgramModule {
  this: StageModule =>

  import cats._
  import cats.data._
  import cats.free.Free._
  import cats.free._
  import cats.implicits._
  import org.apache.spark.sql._
  import org.apache.spark.sql.catalyst.encoders._
  import scala.util.control._
  import scala.reflect.runtime.universe.{TypeTag, typeOf}

  sealed trait ProgramA[A]

  final case class StageAction[T](tableName: TableName,
                                  stage: DataStage[T],
                                  reader: SparkSession => Dataset[T],
                                  writer: Dataset[T] => Unit) extends ProgramA[DataStage[T]]

  type Program[A] = Free[ProgramA, A]

  type DataProgram[T] = Program[DataStage[T]]

  def persist[T](tableName: TableName,
                 stage: DataStage[T],
                 reader: SparkSession => Dataset[T],
                 writer: Dataset[T] => Unit): DataProgram[T] =
    liftF[ProgramA, Stage[Dataset[T]]](StageAction[T](tableName, stage, reader, writer))

  def persist[T: TypeTag](name: String)
                         (stage: DataStage[T])
                         (implicit database: Database): DataProgram[T] = {
    val reader: SparkSession => Dataset[T] = session => {
      if (typeOf[T] == typeOf[Row])
        database.readTable(session, name).asInstanceOf[Dataset[T]]
      else
        database.readTable(session, name).as[T](ExpressionEncoder())
    }

    val writer: Dataset[T] => Unit = dataset => {
      database.writeTable(name, dataset.toDF())
    }

    this.persist(TableName(name, database.schema), stage, reader, writer)
  }

  implicit class StageForProgramExtensions[T: TypeTag](stage: DataStage[T]) {
    def persist(tableName: String)(implicit database: Database): DataProgram[T] =
      ProgramModule.this.persist(tableName)(stage)
  }

  type StageWriter[A] = Writer[List[(TableName, Stage[Unit])], A]

  val programSplitter: ProgramA ~> StageWriter = λ[ProgramA ~> StageWriter] {
    case r: StageAction[t] => {
      val toStage = r.stage.ap(write[t](r.writer, r.tableName))
      for {
        _ <- List((r.tableName, toStage)).tell
      } yield {
        read(r.reader)
      }
    }
  }

  def programAnalyser[L: Monoid](f: StageA ~> λ[α => L]): ProgramA ~> Writer[L, ?] = λ[ProgramA ~> Writer[L, ?]] {
    case r: StageAction[t] => {
      for {
        _ <- r.stage.analyze(f).tell
      } yield {
        r.stage
      }
    }
  }

  private def withJobDescription[T](description: String)(action: => T)(implicit session: SparkSession): T = {
    //TODO: можно укладывать описание на стек.
    session.sparkContext.setJobDescription(description)
    try {
      action
    }
    finally {
      session.sparkContext.setJobDescription(null)
    }
  }

  //TODO: в финальной версии для проверки ридеров надо использовать граф исполнения, а не саму программу.
  def checkReaders[A](program: Program[A])(implicit session: SparkSession): List[Throwable] = {
    type ActionM = Set[ReadAction[_]]
    val kk = program.foldMap(programAnalyser(λ[StageA ~> λ[α => ActionM]] {
      case r: ReadAction[_] => Set(r)
      case _ => Monoid.empty[ActionM]
    }))
    val (reads, _) = kk.run


    val aaa = reads.toList.flatMap { (r: ReadAction[_]) => try {
      withJobDescription("Checking reader")(r.f(session).take(1))
      None
    } catch  {
      case NonFatal(e) => Some(e)
    } }
    aaa
  }

  def exec[A](program: Program[Stage[A]])(implicit session: SparkSession): A = {
    val (stages, finalStage) = program.foldMap(programSplitter).run
    stages.foreach(stage => {
      session.sparkContext.setJobDescription(s"${stage._1.database}.${stage._1.table}")
      try {
        val compiled = stage._2.foldMap(stageCompiler)
        compiled(session)
      } finally {
        session.sparkContext.setJobDescription(null)
      }
    })
    val compiled = finalStage.foldMap(stageCompiler)
    compiled(session)
  }

  import scalax.collection.Graph // or scalax.collection.mutable.Graph
  import scalax.collection.GraphPredef._, scalax.collection.GraphEdge._

  def buildGraph(program: Program[Stage[_]]) = {
    val (stages, _) = program.foldMap(programSplitter).run
    val tableToStage = stages.flatMap { case (stageName, stage) =>  writeActions(stage).map(a => (a.tableName, stageName)).toList  }.toSet


  }
}
