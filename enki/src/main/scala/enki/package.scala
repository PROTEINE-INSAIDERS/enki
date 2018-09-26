import enki.ds.Extensions

package object enki
  extends AllModules
    with stage.Aliases
    // with stage.Syntax
    with stage.Compilers
    with stage.Analyzers
    with application.Aliases
    with application.Implicits
    with writer.Aliases
    with Extensions
    with program.Aliases
    with args.Aliases {
  type SparkAction[A] = Environment => A

  /**
    * Since using SparkImplicits and SparkSession.implicits at once will lead to ambiguity, SparkImplicits not imported by default.
    */
  val implicits: SparkImplicits = new SparkImplicits {}
}