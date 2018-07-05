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

  import scala.reflect.runtime.universe.{TypeTag, typeOf}

  sealed trait ProgramA[A]

  final case class StageAction[T](name: String,
                                  stage: DataStage[T],
                                  reader: SparkSession => Dataset[T],
                                  writer: Dataset[T] => Unit) extends ProgramA[DataStage[T]]

  type Program[A] = Free[ProgramA, A]

  type DataProgram[T] = Program[DataStage[T]]

  def stage[T](name: String,
               stage: DataStage[T],
               reader: SparkSession => Dataset[T],
               writer: Dataset[T] => Unit): DataProgram[T] =
    liftF[ProgramA, Stage[Dataset[T]]](StageAction[T](name, stage, reader, writer))

  def stage[T: TypeTag](name: String)
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

    this.stage(name, stage, reader, writer)
  }

  implicit class StageForProgramExtensions[T: TypeTag](stage: DataStage[T]) {
    def stage(tableName: String)(implicit database: Database): DataProgram[T] =
      ProgramModule.this.stage(tableName)(stage)
  }

  type StageWriter[A] = Writer[List[(String, Stage[Unit])], A]

  val stageExtractor: ProgramA ~> StageWriter = λ[ProgramA ~> StageWriter] {
    case r: StageAction[t] => {
      val toStage = r.stage.ap(write[t](r.writer))
      for {
        _ <- List((r.name, toStage)).tell
      } yield {
        read(r.reader)
      }
    }
  }

  def exec[A](program: Program[Stage[A]])(implicit session: SparkSession): A = {
    val (stages, finalStage) = program.foldMap(stageExtractor).run
    stages.foreach(stage => {
      session.sparkContext.setJobDescription(stage._1)
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
}
