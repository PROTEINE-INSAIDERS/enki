lazy val enki = project in file("enki")

lazy val root = (project in file("."))
  .aggregate(enki)