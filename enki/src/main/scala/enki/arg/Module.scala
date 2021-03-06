package enki
package arg

import java.sql.Timestamp

import scala.reflect.runtime.universe._

trait Module {
  /* module parameters */

  type ArgOp[_]

  /* reexports */

  type ArgAlg[F[_]] = enki.arg.ArgAlg[F]
  val ArgAlg: enki.arg.ArgAlg.type = enki.arg.ArgAlg

  type ArgToOpts = enki.arg.ArgToOpts
  val ArgToOpts: enki.arg.ArgToOpts.type = enki.arg.ArgToOpts

  type ArgOptsBuilder = enki.arg.ArgOptsBuilder
  val ArgOptsBuilder: enki.arg.ArgOptsBuilder.type = enki.arg.ArgOptsBuilder

  type ArgHandler[M[_]] = enki.arg.ArgHandler[M]

  type ParameterValue = enki.arg.ParameterValue

  type BigIntValue = enki.arg.BigIntValue
  val BigIntValue: enki.arg.BigIntValue.type = enki.arg.BigIntValue

  type BooleanValue = enki.arg.BooleanValue
  val BooleanValue: enki.arg.BooleanValue.type = enki.arg.BooleanValue

  type IntegerValue = enki.arg.IntegerValue
  val IntegerValue: enki.arg.IntegerValue.type = enki.arg.IntegerValue

  type StringValue = enki.arg.StringValue
  val StringValue: enki.arg.StringValue.type = enki.arg.StringValue

  type TimestampValue = enki.arg.TimestampValue
  val TimestampValue: enki.arg.TimestampValue.type = enki.arg.TimestampValue

  type Parameters = enki.arg.Parameters
  val Parameters: enki.arg.Parameters.type = enki.arg.Parameters

  /* module functions */

  final def arg[T: TypeTag](
                             name: String,
                             description: String = "",
                             defaultValue: Option[T] = None
                           )
                           (
                             implicit alg: ArgAlg[ArgOp]
                           ): alg.FS[T] = {
    if (typeOf[T] == typeOf[BigInt]) {
      alg.bigint(name, description, defaultValue.asInstanceOf[Option[BigInt]]).asInstanceOf[alg.FS[T]]
    } else if (typeOf[T] == typeOf[Boolean]) {
      alg.bool(name, description, defaultValue.asInstanceOf[Option[Boolean]]).asInstanceOf[alg.FS[T]]
    } else if (typeOf[T] == typeOf[Int]) {
      alg.int(name, description, defaultValue.asInstanceOf[Option[Int]]).asInstanceOf[alg.FS[T]]
    } else if (typeOf[T] == typeOf[String]) {
      alg.string(name, description, defaultValue.asInstanceOf[Option[String]]).asInstanceOf[alg.FS[T]]
    } else if (typeOf[T] == typeOf[Timestamp]) {
      alg.timestamp(name, description, defaultValue.asInstanceOf[Option[Timestamp]]).asInstanceOf[alg.FS[T]]
    } else {
      throw new Exception(s"Argument of type ${typeOf[T]} not supported.")
    }
  }
}
