package enki

import enki.arg.ParameterValue
import org.apache.spark.sql.SparkSession

case class Environment(session: SparkSession, parameters: Map[String, ParameterValue])

object Environment {
  def apply(session: SparkSession): Environment = Environment(session, Map.empty)
}