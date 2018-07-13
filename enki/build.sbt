name := "enki"

scalaVersion := "2.11.12"

val sparkVersion = "2.2.1"
val scalaTestVersion = "3.0.5"
val catsVersion = "1.1.0"
/*
val framelessVersion = sparkVersion match {
  case "2.2.1" => "0.5.2"
  case "2.3.0" => "0.6.1"
}
*/
val scalaGraphVersion = "1.12.5"
val declineVersion = "0.4.2"

scalacOptions += "-Ypartial-unification"

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % catsVersion,
  "org.typelevel" %% "cats-free" % catsVersion,
  "org.scala-graph" %% "graph-core" % scalaGraphVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "com.monovore" %% "decline" % declineVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
)

addCompilerPlugin("org.spire-math" % "kind-projector" % "0.9.7" cross CrossVersion.binary)

licenses += ("BSD-3-Clause", url("http://opensource.org/licenses/BSD-3-Clause"))
publishMavenStyle := true
publishArtifact := true
publishArtifact in Test := false
