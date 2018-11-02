package enki.pm


import java.nio.file.{Path, Paths}

import cats._
import cats.effect._
import cats.implicits._
import com.monovore.decline._
import enki.pm.fs.NioFileSystem
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

    val moduleTree = scheme.anaM(fromFiles.coalgebra).apply(path).unsafeRunSync()

    println("Module tree =================================")
    println(moduleTree)

    val qGen = QualifiedNameGenerator(fromFiles.coalgebra, (path: Path) => FilenameUtils.removeExtension(path.getFileName.toString))

    val moduleTreeWithQNames = scheme.anaM(qGen.coalgebra).apply((None, path)).unsafeRunSync()

    println("Module tree with q-names =================================")
    println(moduleTreeWithQNames)


    // 1. выкинуть не валидные модули.
    // 2. выкинуть пустые модули.

    //  val co = inheritAttributes[IO, Path](
    //     ???, ///Module.fromFilesystem[IO],
    //   Module.moduleNameFromPath)

    //
    //   val tree = scheme.anaM(co).apply((path, None)).unsafeRunSync()
    //  val attr = scheme.cataM(SynthesizedAttributes.algebra[IO]).apply(tree).unsafeRunSync()

    //  println(attr)

    /*
        val moduleReads = Algebra[AbstractModuleTreeF[LogicalPlan, ?], List[String]] {
          case (a: InheritedAttributes, Left(b: Module)) =>
            println(b)
            List.empty[String]
          case (a: InheritedAttributes, Right(b: List[String])) =>
            println(b)
            List.empty[String]
        }

        val compilePlan: AbstractModuleTreeF[Module, ?] ~> AbstractModuleTreeF[LogicalPlan, ?] =
          new (AbstractModuleTreeF[Module, ?] ~> AbstractModuleTreeF[LogicalPlan, ?]) {
            override def apply[A](fa: AbstractModuleTreeF[Module, A]): AbstractModuleTreeF[LogicalPlan, A] = fa match {
              case (a: InheritedAttributes, Left(b: Module)) => AttrF.apply[RoseTreeF[LogicalPlan, ?], InheritedAttributes, A](???, ???)
              // case (a: InheritedAttributes, Right(b: List[A])) => ???
            }
          }
    */
    // val reads = scheme.hyloM(moduleReads.lift[IO], co).apply((path, None)).unsafeRunSync()

    // println(reads )

    // val kk = scheme.ghyloM(
    //   moduleAlg.gather(Gather.cata),
    //   co.scatter(Scatter.ana)
    // )
    //TODO: сделать плоский граф зависимостей.
    //

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