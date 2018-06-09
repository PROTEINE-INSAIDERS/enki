name := "enki"

version := "2.0"

scalaVersion := "2.11.12"
val sparkVersion = "2.2.1"
val scalaTestVersion = "3.0.5"
val paradiseVersion = "2.1.0"
val simulacrumVersion = "0.12.0"
val scalaLoggingVersion = "3.9.0"

scalacOptions += "-Ypartial-unification"

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "1.1.0",
  // "com.github.mpilquist" %% "simulacrum" % simulacrumVersion,
  // "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
)

addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)
