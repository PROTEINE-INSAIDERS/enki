import enki.ds.Extensions
import enki.program.ActionGraphBuilder

//TODO: deprecated: now enki parametrized by operation types and should be instanized by user.
// default instance provided in enki.default.
package object enki
  extends AllModules
    with stage.Aliases
    // with stage.Syntax
    with stage.Compilers
    with writer.Aliases
    with Extensions
    with program.Aliases
    with args.Aliases
{
  type SparkAction[A] = Environment => A

  /**
    * Since using SparkImplicits and SparkSession.implicits at once will lead to ambiguity, SparkImplicits not imported by default.
    */
  val implicits: SparkImplicits = new SparkImplicits {}
}