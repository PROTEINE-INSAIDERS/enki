package enki.pm.project

import java.nio.charset._
import java.nio.file.Path

import enki.pm.fs._
import cats._
import cats.data.Validated._
import cats.implicits._
import enki.pm.internal._
import qq.droste._
import qq.droste.data._
import qq.droste.data.prelude._

case class SynthesizedAttributes(
                                  arguments: Validated[Set[String]]
                                )

object ModuleTreeBuilder {
  type ModuleTreeWithQNamesF[A] = AttrRoseTreeF[String, Validated[Module], A]

  def fromFileSystem[M[_], E <: Throwable](
                                            implicit fileSystem: FileSystem[M],
                                            error: MonadError[M, E]
                                          ): CoalgebraM[M, RoseTreeF[Validated[Module], ?], Path] =
    CoalgebraM[M, RoseTreeF[Validated[Module], ?], Path] { path =>
      Validated.wrapError {
        (fileSystem.isRegularFile(path), fileSystem.isDirectory(path)).tupled >>= {
          case (true, _) => fileSystem.readAllText(path, StandardCharsets.UTF_8) map { sql =>
            RoseTreeF.leaf[Validated[Module], Path](SqlModule(sql).valid)
          }
          case (_, true) => fileSystem.list(path) map RoseTreeF.node[Validated[Module], Path]
          case _ => RoseTreeF.leaf[Validated[Module], Path](s"$path neither file nor dirrectory.".invalidNec).pure[M]
        }
      }.map {
        case Validated.Valid(res) => res
        case Validated.Invalid(err) => RoseTreeF.leaf[Validated[Module], Path](err.invalid)
      }
    }

  // при синтезе q-names доп. носитель должен быть типа (Option[String], String)
  def withQualifiedNames[M[_] : Applicative, A](
                                                 moduleTreeBuilder: CoalgebraM[M, RoseTreeF[Validated[Module], ?], A],
                                                 moduleName: A => String
                                               ): CoalgebraM[M, ModuleTreeWithQNamesF, (A, String)] =
    Attributes.inheritedAttributes(
      moduleTreeBuilder,
      (a, parentName) => s"$parentName.${moduleName(a)}".pure[M]
    )

  val test = Algebra[ModuleTreeWithQNamesF, AttrRoseTree[String, Validated[Module]]] {
    case Attr(qualifiedName: String, tree: AttrRoseTree[String, Validated[Module]]) =>
      println("====== cata alg")
   //   tree match {
    //    case Attr(a, b) =>
    //      println(a)
    //      println(b)
    //  }
      println(tree.getClass)
      println(tree)
      tree
  }

  val withSynthesizedAttributes = Algebra[ModuleTreeWithQNamesF, AttrRoseTree[SynthesizedAttributes, Validated[Module]]] {
    case Attr(qualifiedName: String, tree) => tree match {
      case RoseTreeF.Left(module) =>
        println(module)
        val attributes = SynthesizedAttributes(
          arguments = Set.empty[String].valid
        )
        Attr(
          attributes,
          ???
        )

        /*
        AttrF(
          SynthesizedAttributes(
            arguments = Set.empty[String].valid
        ), ???)
        */
      case RoseTreeF.Right(r) =>
        ???
    }
  }

  // def cleanUp = Algebra[ModuleTreeWithQNamesF, ]
}
