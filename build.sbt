name := "enki"

version := "0.1"

scalaVersion := "2.11.12"
val sparkVersion = "2.2.1"

scalacOptions += "-Ypartial-unification"

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "1.1.0",
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided
)
