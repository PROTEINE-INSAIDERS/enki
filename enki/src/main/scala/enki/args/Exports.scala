package enki
package args

trait Exports {
  type ArgsAlg[F[_]]= args.ArgsAlg[F]
  val ArgsAlg: args.ArgsAlg.type = args.ArgsAlg

  type ArgsToOpts[M] = args.ArgsToOpts[M]

  type ArgsHandler = args.ArgsHandler

  type ArgumentAction = args.ArgumentAction
  type StringArgumentAction = args.StringArgumentAction
  val StringArgumentAction: args.StringArgumentAction.type = args.StringArgumentAction
  type IntegerArgumentAction = args.IntegerArgumentAction
  val IntegerArgumentAction: args.IntegerArgumentAction.type = args.IntegerArgumentAction
  type BooleanArgumentAction = args.BooleanArgumentAction
  val BooleanArgumentAction: args.BooleanArgumentAction.type = args.BooleanArgumentAction
}
