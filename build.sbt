val sparkVersion = "2.2.1"
val scalaGraphVersion = "1.12.5"

resolvers += Resolver.url("scoverage-bintray", url("https://dl.bintray.com/sksamuel/sbt-plugins/"))(Resolver.ivyStylePatterns)

lazy val enki = project in file("enki")

lazy val tutorial = (project in file("demos/tutorial")).dependsOn(enki)
  .settings(
    scalaVersion := "2.11.12",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion
    )
  )

lazy val root = (project in file("."))
  .aggregate(enki, tutorial)


