package enki
package args

trait Aliases {
  type ArgsAlg[F[_]] = args.Args[F]
  val ArgsAlg: args.Args.type = args.Args

  type ArgsToOpts[M] = args.ArgsToOpts[M]

  type ArgsCompiler = args.ArgsCompiler

  type ArgumentAction = args.ArgumentAction
  type StringArgumentAction = args.StringArgumentAction
  val StringArgumentAction: args.StringArgumentAction.type = args.StringArgumentAction
  type IntegerArgumentAction = args.IntegerArgumentAction
  val IntegerArgumentAction: args.IntegerArgumentAction.type = args.IntegerArgumentAction
  type BooleanArgumentAction = args.BooleanArgumentAction
  val BooleanArgumentAction: args.BooleanArgumentAction.type = args.BooleanArgumentAction
}
