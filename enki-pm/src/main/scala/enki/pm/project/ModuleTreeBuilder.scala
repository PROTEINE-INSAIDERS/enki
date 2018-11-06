package enki.pm.project

import cats._
import cats.data.Chain
import cats.implicits._
import enki.pm.internal._
import org.apache.log4j.Level
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import qq.droste._
import qq.droste.data.prelude._

import scala.util.Try

case class SynthesizedAttributes(
                                  arguments: Validated[Set[String]]
                                )

case class InheritedAttributes(
                                qualifiedName: Chain[String]
                              )

object InheritedAttributes {
  def apply(): InheritedAttributes = InheritedAttributes(qualifiedName = Chain.empty)
}

abstract class ModuleTreeBuilder[M[_] : Monad, A] {
  private val nonsubstitutingParser: SparkSqlParser = new SparkSqlParser(new SQLConf()) {
    // shut the fuck up fucking logger.
    org.apache.log4j.Logger.getLogger(this.logName).setLevel(Level.OFF)
  }

  private def newAttributes(moduleName: String, inheritedAttributes: InheritedAttributes): InheritedAttributes = {
    InheritedAttributes(
      qualifiedName = inheritedAttributes.qualifiedName :+ moduleName
    )
  }

  protected def sqlModule(sql: String, attr: InheritedAttributes): Validated[SqlModule] = {
    Validated.catchNonFatal(nonsubstitutingParser.parsePlan(sql)) map { logicalPlan =>
      SqlModule(
        qualifiedName = attr.qualifiedName,
        sql = sql,
        logicalPlan = logicalPlan
      )
    }
  }

  protected def step(carrier: A, attributes: InheritedAttributes): M[RoseTreeF[Validated[Module], A]]

  protected def getModuleName(carrier: A): M[String]

  def coalgebra: CoalgebraM[M, RoseTreeF[Validated[Module], ?], (A, InheritedAttributes)] =
    CoalgebraM[M, RoseTreeF[Validated[Module], ?], (A, InheritedAttributes)] { case (carrier, inheritedAttributes) =>
      for {
        moduleName <- getModuleName(carrier)
        attributes = newAttributes(moduleName, inheritedAttributes)
        layer <- step(carrier, attributes)
      } yield layer fmap {
        (_, attributes)
      }
    }
}


/*
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
}
*/