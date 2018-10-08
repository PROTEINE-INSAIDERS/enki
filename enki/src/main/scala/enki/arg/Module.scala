package enki
package arg

import scala.reflect.runtime.universe._

trait Module {
  /* module parameters */

  type ArgOp[_]

  /* reexports */

  type ArgAlg[F[_]] = enki.arg.ArgAlg[F]
  val ArgAlg: enki.arg.ArgAlg.type = enki.arg.ArgAlg

  type ArgToOpts = enki.arg.ArgToOpts
  val ArgToOpts: enki.arg.ArgToOpts.type = enki.arg.ArgToOpts

  type ArgHandler = enki.arg.ArgHandler
  val ArgHandler: enki.arg.ArgHandler.type = enki.arg.ArgHandler

  /* module functions */

  final def arg[T: TypeTag](
                             name: String,
                             description: String = "",
                             defaultValue: Option[T] = None
                           )
                           (
                             implicit alg: ArgAlg[ArgOp]
                           ): alg.FS[T] = {
    if (typeOf[T] == typeOf[Boolean]) {
      alg.bool(name, description, defaultValue.asInstanceOf[Option[Boolean]]).asInstanceOf[alg.FS[T]]
    } else if (typeOf[T] == typeOf[Int]) {
      alg.int(name, description, defaultValue.asInstanceOf[Option[Int]]).asInstanceOf[alg.FS[T]]
    } else if (typeOf[T] == typeOf[String]) {
      alg.string(name, description, defaultValue.asInstanceOf[Option[String]]).asInstanceOf[alg.FS[T]]
    } else {
      throw new Exception(s"Argument of type ${typeOf[T]} not supported.")
    }
  }
}
