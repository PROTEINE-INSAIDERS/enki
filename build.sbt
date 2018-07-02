name := "enki"

scalaVersion := "2.11.12"
val sparkVersion = "2.2.1"
val scalaTestVersion = "3.0.5"
val catsVersion = "1.1.0"
val catsEffectVersion = "1.0.0-RC2"
val framelessVersion = sparkVersion match {
  case "2.2.1" => "0.5.2"
  case "2.3.0" => "0.6.1"
}

scalacOptions += "-Ypartial-unification"

libraryDependencies ++= Seq(
  "org.atnos" %% "eff" % "5.1.0",
  "org.typelevel" %% "cats-core" % catsVersion,
  "org.typelevel" %% "cats-free" % catsVersion,
  "org.typelevel" %% "cats-effect" % catsEffectVersion,
  "org.scala-graph" %% "graph-core" % "1.12.5",
  "org.typelevel" %% "frameless-dataset" % framelessVersion,
  "org.typelevel" %% "frameless-cats" % framelessVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
)

addCompilerPlugin("org.spire-math" % "kind-projector" % "0.9.7" cross CrossVersion.binary)
