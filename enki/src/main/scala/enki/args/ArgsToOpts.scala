package enki.args

import cats._
import cats.data._
import com.monovore.decline._
import enki._

class ArgsToOpts[M: Monoid](f: Opts[(String, ParameterValue)] => M) extends ArgsAlg.Handler[Const[M, ?]] {

  private def option[A: Argument](
                                   name: String,
                                   description: String,
                                   defaultValue: Option[A],
                                   inject: A => ParameterValue
                                 ): Const[M, A] = {
    val opt = Opts.option[A](long = name, help = description)
    val res = defaultValue match {
      case Some(value) => opt.withDefault(value)
      case None => opt
    }
    Const(f(res.map { (value: A) => (name, inject(value)) }))
  }

  override def bool(
                     name: String,
                     description: String,
                     defaultValue: Option[Boolean]
                   ): Const[M, Boolean] = {
    val res = defaultValue match {
      case None | Some(false) => Opts.flag(name, description).orFalse
      case Some(true) =>
        Opts
          .option[String](name, description, metavar = "boolean")
          .withDefault("true")
          .mapValidated {
            case "true" => Validated.valid(true)
            case "false" =>Validated.valid( false)
            case other => Validated.invalidNel("Expecting `true' of `false'.")
          }
    }

    Const(f(res.map { a => (name, BooleanValue(a)) }))
  }

  override def int(name: String,
                   description: String,
                   defaultValue: Option[Int]): Const[M, Int] = {
    option(name, description, defaultValue, IntegerValue)
  }

  override def string(name: String,
                      description: String,
                      defaultValue: Option[String]): Const[M, String] = {
    option(name, description, defaultValue, StringValue)
  }
}

