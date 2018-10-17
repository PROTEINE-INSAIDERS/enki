package enki.spark.plan

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal._

// PlanAnalyzer используется в основном интерпретаторе и spark-анализаторах.
// Для построения параметров в enkiMain необходимо анализировать граф (как минимум на reads и writes),
// следовательно к моменту запуска все параметры должны быть заданы.
// Что делает использование spark.sql.variable.substitute несовместимым как минимум с enkiApp.
//TODO: нам в любом случае потребуется анализировать запросы, содержащие переменные, как вариант в процессе анализа
// можно заменять переменные некоторыми специальными значениями.
trait PlanAnalyzer {
  def parser: AbstractSqlParser = new SparkSqlParser(new SQLConf())

  def parsePlan(sqlText: String): LogicalPlan = {
    parser.parsePlan(sqlText)
  }

  def tableReads(plan: LogicalPlan ): Seq[TableIdentifier] = {
    plan.collect { case (a : UnresolvedRelation) => a.tableIdentifier  }
  }
}
