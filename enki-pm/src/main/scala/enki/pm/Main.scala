package enki.pm

import cats._
import cats.effect._
import cats.implicits._
import com.monovore.decline._
import enki.pm.cli._

object MainMain extends IOApp {
  private val name = "enki-pm"
  private val header = "Enki package manager."
  private val version = ""
  private val helpFlag = true

  def main(): Opts[IO[ExitCode]] = Opts {
    def test[F[_] : Monad](implicit c: Console[F]) = for {
      _ <- c.printLn("test1")
      _ <- c.printLn("test2")
    } yield ()

    type PMMonad[A] = IO[A]

    implicit val console = new SystemConsole[PMMonad]()
    implicit val prompt = new AutoPrompt[PMMonad]()

    def test2[F[_]](implicit p: Prompt[F]) = for {
      a <- prompt.ask(WhereDoYouWantToGoToday())
    } yield a


    test2[PMMonad].as(ExitCode.Success)
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