package enki.pm

import java.io.File
import java.nio.file.{Path, Paths}

import cats._
import cats.effect._
import cats.implicits._
import com.monovore.decline._
import enki.pm.fs.NioFileSystem
import enki.pm.project._
import qq.droste._
import qq.droste.data._
import qq.droste.data.prelude._

case class InheritedAttributes(
                                file: File,
                                parentModuleName: Option[String]
                              )

object O {

  /**
    * Package module to be deployed.
    */
  case class DerivedAttributes(
                                qualifiedName: String
                              )

  /**
    * Module tree abstracted over fixpoint type A.
    *
    * Module tree is a Rose Tree with modules in leaf nodes where each node associated with derived attributes.
    */
  type ModuleTreeF[A] = AttrF[RoseTree[Module, ?], DerivedAttributes, A]

  /**
    * Carrier type of module coalgebra (product of optional derived attributes and underlying coalgebra carrier A).
    */
  type ModuleCoalgebraCarrier[A] = (A, Option[DerivedAttributes])

  /**
    * Module coalgebra.
    *
    * CoalgebraM on ModuleTreeF with ModuleCoalgebraCarrier abstracted over functor F monad and carrier type A.
    */
  type ModuleCoalgebra[F[_], A] = CoalgebraM[F, ModuleTreeF, ModuleCoalgebraCarrier[A]]

  /**
    * Annotate module coalgebra with inherited attributes.
    *
    * @param moduleName Extract module name from carrier
    * @tparam F Functor type
    * @tparam A Module coalgebra carrier type.
    * @return Module coalgebra.
    */
  def inheritAttributes[F[_] : Applicative, A](
                                                underlyingCoalgebra: Module.Coalgebra[F, A],
                                                moduleName: A => String
                                              ): ModuleCoalgebra[F, A] = CoalgebraM[F, ModuleTreeF, ModuleCoalgebraCarrier[A]] {
    a: ModuleCoalgebraCarrier[A] =>
      val carrier = a._1
      val derivedAttributes = a._2

      def getQualifiedName(moduleName: String, parentModuleNameOpt: Option[String]): String = parentModuleNameOpt match {
        case Some(parentModuleName) => s"$parentModuleName.$moduleName"
        case None => moduleName
      }

      val attributes = DerivedAttributes(
        qualifiedName = getQualifiedName(moduleName(carrier), derivedAttributes.map(_.qualifiedName))
      )

      underlyingCoalgebra.run(carrier).map {
        case Left(module: Module) =>
          AttrF[CoattrF[List, Module, ?], DerivedAttributes, ModuleCoalgebraCarrier[A]](
            attributes,
            CoattrF.pure[List, Module, ModuleCoalgebraCarrier[A]](module))
        case Right(xs: List[A]) =>
          AttrF[CoattrF[List, Module, ?], DerivedAttributes, ModuleCoalgebraCarrier[A]](
            attributes,
            CoattrF.roll[List, Module, ModuleCoalgebraCarrier[A]](xs.map(c => (c, Some(attributes)))))
      }
  }
}

object MainMain extends IOApp {
  private val name = "enki-pm"
  private val header = "Enki package manager."
  private val version = ""
  private val helpFlag = true

  def main(): Opts[IO[ExitCode]] = Opts {
    implicit val fileSystem = NioFileSystem[IO]()

    val co = O.inheritAttributes[IO, Path](
      Module.fromFilesystem[IO],
      Module.moduleNameFromPath)

    val path = Paths.get(System.getProperty("user.home"), "Projects/test-enki-project")
    val res = scheme.anaM(co).apply((path, None)).unsafeRunSync()
    println("=================================")
    println(res)


    //   val co = new ModuleCoalgebra[IO, Path]()
    //
    //  val ana = scheme.anaM(co.coalgebra).apply((path, None))
    /*
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

    // Развёртывание идёт двумя путями  pure и roll. pure генерирует модуль, roll - список наследуемых атрибутов.
    // При свёртывании будут видны только модули, но не наследуемые атрибуты (возможно, это плохо).

    // можно попробовать CoattrF[List, InheritedAttributes, ?], InheritedAttributes


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
*/
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