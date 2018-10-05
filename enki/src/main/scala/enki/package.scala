

/**
  * Enki static type aliases
  */
package object enki
  extends AllModules
    with spark.Exports
    // with stage.Syntax
    with spark.Compilers
    with program.Exports
    with Aliases {

}