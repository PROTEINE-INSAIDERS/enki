import cats.~>
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

package object enki
  extends AllModules {
  type SparkAction[A] = SparkSession => A


  def findSourceStage:ActionGraph => Set[enki.ReadAction[_]] = graph =>
    graph.linearized
      .flatMap(name => stageReads(graph(name)))
      .map(action => (s"${action.schemaName}.${action.tableName}", action))
      .toSet
      .filter(m => !graph.linearized.contains(m._1)).map(_._2)


  def generateStateOfDatabase: (StageAction[_], SparkSession) => Unit =
    (action, session) =>
      action match {
        case readAction: ReadAction[t] =>
          if (!session.catalog.databaseExists(readAction.schemaName)) session.sql(s"create database ${readAction.schemaName}")
          val encoder = ExpressionEncoder[t]()(readAction.tag)
          session.emptyDataset[t](encoder).write.saveAsTable(s"${readAction.schemaName}.${readAction.tableName}")
      }

}