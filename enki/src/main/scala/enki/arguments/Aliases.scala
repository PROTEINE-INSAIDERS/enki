package enki
package arguments

trait Aliases {
  type Arguments[F[_]] = arguments.Arguments[F]
  val Arguments: arguments.Arguments.type = arguments.Arguments
  type ArgumentsToOpts[M] = arguments.ArgumentsToOpts[M]
}
