resolvers += Resolver.url("scoverage-bintray", url("https://dl.bintray.com/sksamuel/sbt-plugins/"))(Resolver.ivyStylePatterns)

lazy val enki = project in file("enki")

lazy val root = (project in file("."))
  .aggregate(enki)


