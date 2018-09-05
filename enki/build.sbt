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
val kindProjectorVersion = "0.9.6"
val shapelessVersion = "2.3.3"
val contextualVersion = "1.1.0"

scalacOptions ++= Seq(
  "-deprecation",                      // Emit warning and location for usages of deprecated APIs.
  "-encoding", "utf-8",                // Specify character encoding used by source files.
  "-explaintypes",                     // Explain type errors in more detail.
  "-feature",                          // Emit warning and location for usages of features that should be imported explicitly.
  "-language:existentials",            // Existential types (besides wildcard types) can be written and inferred
  "-language:experimental.macros",     // Allow macro definition (besides implementation and application)
  "-language:higherKinds",             // Allow higher-kinded types
  "-language:implicitConversions",     // Allow definition of implicit functions called views
  "-unchecked",                        // Enable additional warnings where generated code depends on assumptions.
  "-Xcheckinit",                       // Wrap field accessors to throw an exception on uninitialized access.
 // "-Xfatal-warnings",                  // Fail the compilation if there are any warnings.
  "-Xfuture",                          // Turn on future language features.
  "-Xlint:adapted-args",               // Warn if an argument list is modified to match the receiver.
  "-Xlint:by-name-right-associative",  // By-name parameter of right associative operator.
  "-Xlint:delayedinit-select",         // Selecting member of DelayedInit.
  "-Xlint:doc-detached",               // A Scaladoc comment appears to be detached from its element.
  "-Xlint:inaccessible",               // Warn about inaccessible types in method signatures.
  "-Xlint:infer-any",                  // Warn when a type argument is inferred to be `Any`.
  "-Xlint:missing-interpolator",       // A string literal appears to be missing an interpolator id.
  "-Xlint:nullary-override",           // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Xlint:nullary-unit",               // Warn when nullary methods return Unit.
  "-Xlint:option-implicit",            // Option.apply used implicit view.
//  "-Xlint:package-object-classes",     // Class or object defined in package object.
  "-Xlint:poly-implicit-overload",     // Parameterized overloaded implicit methods are not visible as view bounds.
  "-Xlint:private-shadow",             // A private field (or class parameter) shadows a superclass field.
  "-Xlint:stars-align",                // Pattern sequence wildcard must align with sequence component.
  "-Xlint:type-parameter-shadow",      // A local type parameter shadows a type already in scope.
  "-Xlint:unsound-match",              // Pattern match may not be typesafe.
  "-Yno-adapted-args",                 // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
  "-Ypartial-unification",             // Enable partial unification in type constructor inference
  "-Ywarn-dead-code",                  // Warn when dead code is identified.
  "-Ywarn-inaccessible",               // Warn about inaccessible types in method signatures.
  "-Ywarn-infer-any",                  // Warn when a type argument is inferred to be `Any`.
  "-Ywarn-nullary-override",           // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Ywarn-nullary-unit",               // Warn when nullary methods return Unit.
  "-Ywarn-numeric-widen",              // Warn when numerics are widened.
  "-Ywarn-value-discard"               // Warn when non-Unit expression results are unused.
)

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % catsVersion, // коты
  "org.typelevel" %% "cats-free" % catsVersion, // свободные монадки и апликативные функторы для Stage
  "org.scala-graph" %% "graph-core" % scalaGraphVersion, // граф для представления зависимостей
  "com.monovore" %% "decline" % declineVersion, // парсер командной строки для EnkiApp
  "com.chuusai" %% "shapeless" % shapelessVersion, // HLIST для генерации дефолтных данных (в enki.test)
  "com.propensive" %% "contextual" % contextualVersion, // интерполятор для enki.test
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided, // Спарк
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided, // Спарк SQL
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test", // тестирование

//  "com.slamdata" %% "matryoshka-core" % "0.18.3" , // матрёшка для тестирования рекурсивных схем по stage - дереву (вытянет за собой scalaz)
//  "io.higherkindness" %% "droste-core" % "0.4.0" , // рекурсивные схемы для котов
  "io.frees" %% "frees-core"               % "0.8.2",
  "io.frees" %% "frees-effects"            % "0.8.2",
  "io.frees" %% "frees-cache"              % "0.8.2",
  compilerPlugin("org.scalameta" % "paradise" % "3.0.0-M10" cross CrossVersion.full),
  compilerPlugin("org.spire-math" % "kind-projector" % kindProjectorVersion cross CrossVersion.binary), // красная лямбда
  compilerPlugin("com.github.mpilquist" %% "local-implicits" % "0.3.0")
)

licenses += ("BSD-3-Clause", url("http://opensource.org/licenses/BSD-3-Clause"))
publishMavenStyle := true
publishArtifact := true
publishArtifact in Test := false

sources in (Compile,doc) := Seq.empty
publishArtifact in (Compile, packageDoc) := false
