package enki.args

import cats._
import cats.data._
import com.monovore.decline._
import enki._

class ArgsToOpts[M: Monoid](f: Opts[(String, ParameterValue)] => M) extends Args.Handler[Const[M, ?]] {

  private def option[A: Argument](
                                   name: String,
                                   description: String,
                                   defaultValue: Option[A],
                                   inject: A => ParameterValue
                                 ): Const[M, A] = {
    val opt = Opts.option[A](long = name, help = description)
    val res = (defaultValue match {
      case Some(value) => opt.withDefault(value)
      case None => opt
    }).map { (value: A) => (name, inject(value)) }
    Const(f(res))
  }

  override def string(name: String,
                      description: String,
                      defaultValue: Option[String]): Const[M, String] = {
    option(name, description, defaultValue, StringValue)
  }

  override def int(name: String,
                   description: String,
                   defaultValue: Option[Int]): Const[M, Int] = {
    option(name, description, defaultValue, IntegerValue)
  }
}

