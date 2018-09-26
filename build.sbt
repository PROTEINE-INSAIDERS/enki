val sparkVersion = "2.2.1"
val scalaGraphVersion = "1.12.5"

resolvers += Resolver.url("scoverage-bintray", url("https://dl.bintray.com/sksamuel/sbt-plugins/"))(Resolver.ivyStylePatterns)

lazy val enki = project in file("enki")

lazy val tutorial = (project in file("demos/tutorial")).dependsOn(enki)
  .settings(
    scalaVersion := "2.11.12",
    scalacOptions ++= Seq("-Ypartial-unification"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion, //TODO: provided?
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      compilerPlugin("org.scalameta" % "paradise" % "3.0.0-M10" cross CrossVersion.full), // required to expand freestyle's macros
      compilerPlugin("org.spire-math" % "kind-projector" % "0.9.6" cross CrossVersion.binary)
    )
  )

lazy val root = (project in file("."))
  .aggregate(enki, tutorial)


