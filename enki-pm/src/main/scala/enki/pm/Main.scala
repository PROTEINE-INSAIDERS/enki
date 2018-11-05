package enki.pm


import java.nio.file.{Path, Paths}

import cats._
import cats.effect._
import cats.implicits._
import com.monovore.decline._
import enki.pm.fs.NioFileSystem
import enki.pm.internal._
import enki.pm.project._
import org.apache.commons.io.FilenameUtils
import qq.droste._
import qq.droste.data._
import qq.droste.data.prelude._

object Main extends IOApp {
  private val name = "enki-pm"
  private val header = "Enki package manager."
  private val version = ""
  private val helpFlag = true

  def algPre[F[_], G[_], A](alg: Algebra[G, A], f: F ~> G) = {
    Algebra[F, A] { a => alg(f(a)) }
  }

  def main(): Opts[IO[ExitCode]] = Opts {

    implicit val fileSystem = NioFileSystem[IO]()

    val fromFiles = new FileSystemModuleTreeBuilder[IO, Throwable]

    val path = Paths.get(System.getProperty("user.home"), "Projects/test-enki-project")

    val qGen = ModuleTreeBuilder.withQualifiedNames(
      fromFiles.coalgebra,
      (path: Path) => FilenameUtils.removeExtension(path.getFileName.toString)
    )
    val moduleTreeWithQNames: AttrRoseTree[String, Validated[Module]] = scheme.anaM(qGen).apply((path, "root")).unsafeRunSync()

  //  println("==== attr")
  //  println(Annotating.attr(moduleTreeWithQNames))

  //  println("==== strip")
  //  println(Annotating.strip(moduleTreeWithQNames))

    println("==== strip all")
    // a: Coattr[List, Validated[Module]]  - но это не выводится.
    val a = Annotating.stripAll[CoattrF[List, Validated[Module], ?], String](moduleTreeWithQNames)
    println(a.getClass)
    println(a.isInstanceOf[CoattrF[List, Validated[Module], Fix[CoattrF[List, Validated[Module], ?]]]])

    println("=== syntethise")
    val ttt = Annotating.synthesize[CoattrF[List, Validated[Module], ?], Int](
      { a => // a: CoattrF[List, Validated[Module], Int]
        println(a)
        0 },
      a
    )

    //  val test = scheme.cata(ModuleTreeBuilder.test).apply(moduleTreeWithQNames)

  //  println("Test =================================")
  //  println(test)


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