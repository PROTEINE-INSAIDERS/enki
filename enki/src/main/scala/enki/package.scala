package object enki
  extends AllModules
    with stage.Aliases
    with application.Aliases
    with application.Implicits {
  type SparkAction[A] = Environment => A

  /**
    * Since using SparkImplicits and SparkSession.implicits at once will lead to ambiguity, SparkImplicits not imported by default.
    */
  val implicits: SparkImplicits = new SparkImplicits {}
}