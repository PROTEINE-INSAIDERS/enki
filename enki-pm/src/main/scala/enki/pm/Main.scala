package enki.pm

import cats._
import cats.effect._
import cats.free.Trampoline
import cats.implicits._
import cats.mtl.{DefaultFunctorTell, FunctorTell}
import com.monovore.decline._
import enki.pm.cli._
import enki.pm.fs.NioFileSystem
import enki.pm.internal._
import enki.pm.project._
import qq.droste._
import qq.droste.data.prelude._
import io.chrisdavenport.log4cats._

object Main extends IOApp {
  private val name = "enki-pm"
  private val header = "Enki package manager."
  private val version = ""
  private val helpFlag = true

  def main(): Opts[IO[ExitCode]] = Opts {
    implicit val console: Console[IO] = new SystemConsole[IO]()
    implicit val logger: Logger[IO] = CliLogger[IO, Throwable]

    for {
      project <- Workflow.bootstrap[IO]
    } yield ExitCode.Success
    /*
    implicit val questions = new PromptQuestions()
    implicit val parsers = new PromptParsers()
    implicit val console = new SystemConsole[IO]()
    implicit val prompt = new CliPrompt[IO, Throwable]()
    implicit val fileSystem = new NioFileSystem[IO]()
    implicit val logger = CliLogger[IO, Throwable]()
    implicit val vLogger = ValidationErrorLogger[IO]()

    implicit val validationLogger = new DefaultFunctorTell[IO, ValidationError] {
      override val functor: Functor[IO] = implicitly

      override def tell(l: ValidationError): IO[Unit] = ???
    }

    val moduleTreeBuilder = new FileSystemModuleTreeBuilder[IO, Throwable]()
    val invalidModuleFilter = InvalidModuleFilter[IO]()

    // 1. Построить дерево, вычислить наследуемые атрибуты.
    // 2. "Почистить" дерево.
    // 3. Вычислить синтезируемые атрибуты.

    // Атрибуты 3-х типов:
    // 1. наследуемые атрибуты.
    // 2. атрибуты модуля.
    // 3. синтезируемые атрибуты.

    // К наследуемым атрибутам относятся все не синтезируемые атрибуты.

    // Наследуемые атрибуты нужны только на этапе построения дерева, чтобы добавить их к модулю.
    // Некоторые модули имеют общий набор наследуемых атрибутов (например, название модуля), также есть
    // уникальные, зависящие от типа модуля.

    // Процедура очистки.
    // 1. Так как мы не знаем, какие именно свойства модуля и наследуемые аттрибуты будут использоваться,
    // единственным адекватным вариантом очистки будет удаление всех модулей и наследуемых атрибутов, при
    // вычислении которых произошли ошибки.

    for {
      projectDir <- prompt.projectDir
      projectTree <- scheme.anaM(moduleTreeBuilder.coalgebra).apply((projectDir, InheritedAttributes()))
      validatedTree <- scheme.cataM(invalidModuleFilter.algebra).apply(projectTree)
    } yield {
      println(validatedTree)
      ExitCode.Success
    }
    */
  }


  override def run(args: List[String]): IO[ExitCode] = {
    val command: Command[IO[ExitCode]] = {
      val showVersion = if (version.isEmpty)
        Opts.never
      else
        Opts.flag("version", "Print the version number and exit.", visibility = Visibility.Partial)
          .map { _ => IO(System.out.println(version)).as(ExitCode.Success) }
      Command(name, header, helpFlag)(showVersion orElse main)
    }
    command.parse(PlatformApp.ambientArgs getOrElse args) match {
      case Left(help) => IO {
        System.err.println(help)
      }.as(ExitCode.Error)
      case Right(action) => action
    }
  }
}