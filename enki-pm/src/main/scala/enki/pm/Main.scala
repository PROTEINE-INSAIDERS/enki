package enki.pm

import cats._
import cats.effect._
import cats.implicits._
import com.monovore.decline._
import enki.pm.cli._
import enki.pm.project.Project

object MainMain extends IOApp {
  private val name = "enki-pm"
  private val header = "Enki package manager."
  private val version = ""
  private val helpFlag = true

  def main(): Opts[IO[ExitCode]] = Opts {
    type PMM[A] = IO[A]

    implicit val console = new SystemConsole[PMM]()
    implicit val formatter = new DefaultFormatter[PMM, Throwable]()
    implicit val q = new PromptQuestions()
    implicit val prompt = new BootstrapPrompt[PMM]()

    def test2[F[_]]= for {
      a <- prompt.whereDoYouWantToGoToday
    } yield a

    test2[PMM].as(ExitCode.Success)

 //   val project = Project.cliProject()

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