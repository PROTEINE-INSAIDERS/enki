package enki.pm.project

import cats._
import cats.data.{Validated => _}
import cats.implicits._
import enki.pm.internal._
import enki.pm.plan.LogicalPlanAnalyser
import org.apache.log4j.Level
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import qq.droste._
import qq.droste.data.prelude._

case class SynthesizedAttributes(
                                  arguments: Validated[Set[String]]
                                )

case class InheritedAttributes(
                                qualifiedName: ModuleQName
                              )

object InheritedAttributes {
  def apply(): InheritedAttributes = InheritedAttributes(qualifiedName = ModuleQName.empty)
}

abstract class ModuleTreeBuilder[M[_] : Monad, A] {
  private val nonsubstitutingParser: SparkSqlParser = new SparkSqlParser(new SQLConf()) {
    // shut the fuck up fucking logger.
    org.apache.log4j.Logger.getLogger(this.logName).setLevel(Level.OFF)
  }

  private val REF_RE = "\\$\\{(?:(\\w+?):)?(\\S+?)\\}".r

  private def findArguments(sql: String): Set[String] = {
    REF_RE.findAllMatchIn(sql).map { m =>
      val prefix = m.group(1)
      val name = m.group(2)
      if (prefix == null) name else s"$prefix:$name"
    }.toSet
  }

  private def findWrites(logicalPlan: LogicalPlan): Set[TableIdentifier] = {
    Set.empty
  }

  private def addModuleNameToErrorMessage[A](a: Validated[A], moduleName: ModuleQName): Validated[A] =
    a.leftMap(_.prepend(s"Error validating module ${moduleName.show}"))

  private def newAttributes(moduleName: String, inheritedAttributes: InheritedAttributes): InheritedAttributes = {
    InheritedAttributes(
      qualifiedName = inheritedAttributes.qualifiedName :+ moduleName
    )
  }

  protected def sqlModule(sql: String, attr: InheritedAttributes): Validated[SqlModule] = {
    addModuleNameToErrorMessage(
      Validated.catchNonFatal(nonsubstitutingParser.parsePlan(sql)) map { plan =>
        SqlModule(
          qualifiedName = attr.qualifiedName,
          sql = sql,
          plan = plan,
          arguments = findArguments(sql),
          reads = LogicalPlanAnalyser.reads(plan),
          writes = Set.empty
        )
      }, attr.qualifiedName
    )
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