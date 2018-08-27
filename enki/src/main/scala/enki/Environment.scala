package enki

import org.apache.spark.sql.SparkSession

case class Environment(session: SparkSession, parameters: Map[String, ParameterValue])