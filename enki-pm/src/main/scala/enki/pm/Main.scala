package enki.pm

import java.io.File
import java.nio.file.Files

import cats.effect._
import cats.implicits._
import com.monovore.decline._
import enki.pm.cli._
import qq.droste._
import qq.droste.data._
import qq.droste.data.prelude._
import org.apache.commons.io.FilenameUtils

case class Module(qualifiedName: String)

case class InheritedAttributes(
                                file: File,
                                parentModuleName: Option[String]
                              )

object MainMain extends IOApp {
  private val name = "enki-pm"
  private val header = "Enki package manager."
  private val version = ""
  private val helpFlag = true

  def mkQualifiedName(file: File, parentModuleName: Option[String]) = {
    val name = FilenameUtils.removeExtension(file.getName)
    parentModuleName match {
      case Some(p) => s"$p.$name"
      case None => name
    }
  }

  def moduleFromFile[F[_]](file: File): F[Option[Module]] = {
    ???
  }


  def main(): Opts[IO[ExitCode]] = Opts {
    type PMM[A] = IO[A]

    implicit val console = new SystemConsole[PMM]()
    implicit val q = new PromptQuestions()
    implicit val p = new PromptParsers()
    implicit val prompt = new CliPrompt[PMM, Throwable]()
    implicit val logger = new CliLogger[PMM, Throwable]()

    // задача:
    // 1. развернуть.
    // 2. вычислить все атрибуты
    // 3. свернуть.

    // Наследуемые атрибут считаем F-коалгебрами, синтезируемые F-алгебрами.
    // артибуты передаются в аккумуляторе, который является произведением из iota, с каждой алгеброй должны также передаваться
    // функци проекции и инъекции.

    // во время развёртывания могут появиться "пустые" ветки, их надо будет убрать во время свёртывания.

    def coa = CoalgebraM[IO, CoattrF[List, Module, ?], InheritedAttributes] {
      case InheritedAttributes(file, parentModuleName) =>
        if (file.isFile) {
          val moduleName = mkQualifiedName(file, parentModuleName)
          CoattrF.pure[List, Module, InheritedAttributes](Module(moduleName)).pure[IO]
        } else if (file.isDirectory) {
          val moduleName = mkQualifiedName(file, parentModuleName)
          for {
            filez <- IO(Option(file.listFiles()).map(_.toList.filterNot(f => Files.isSymbolicLink(f.toPath))).getOrElse(List.empty))
            _ <- IO(println(filez))
          } yield CoattrF.roll[List, Module, InheritedAttributes](filez.map(f => InheritedAttributes(f, Some(moduleName))))
        } else {
          CoattrF.roll[List, Module, InheritedAttributes](List.empty).pure[IO]
        }
    }

    val test1 = scheme.anaM(coa).apply(InheritedAttributes(
      file = new File(System.getProperty("user.home"), "Projects/test-enki-project"),
      parentModuleName = None
    )).unsafeRunSync()

    println(test1)

    ExitCode.Success.pure[IO]
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