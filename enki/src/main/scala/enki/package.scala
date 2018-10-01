import cats.data.Reader
import enki.ds.Extensions

/**
  * Enki static type aliases
  */
package object enki
  extends AllModules
    with stage.Aliases
    // with stage.Syntax
    with stage.Compilers
    with Extensions
    with program.Aliases
    with args.Aliases
with Aliases {

}