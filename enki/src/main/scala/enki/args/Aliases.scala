package enki
package args

trait Aliases {
  type Args[F[_]] = args.Args[F]
  val Args: args.Args.type = args.Args
  type ArgsToOpts[M] = args.ArgsToOpts[M]

  type ArgsCompiler =  args.ArgsCompiler
}
