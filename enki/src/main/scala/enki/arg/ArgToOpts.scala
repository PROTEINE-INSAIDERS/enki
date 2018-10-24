package enki.arg

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import cats._
import cats.data._
import cats.implicits._
import com.monovore.decline._
import freestyle.free._

import scala.util.Try
import scala.util.control.NonFatal

trait ArgToOpts extends FSHandler[ArgAlg.Op, Const[ArgOptsBuilder, ?]] {

  private def option[A: Argument](
                                   name: String,
                                   op: ArgAlg.Op[_],
                                   description: String,
                                   defaultValue: Option[A],
                                   inject: A => ParameterValue
                                 ): Const[ArgOptsBuilder, A] = {
    val opt = Opts.option[A](long = name, help = description)
    val res = defaultValue match {
      case Some(value) => opt.withDefault(value)
      case None => opt
    }
    Const(ArgOptsBuilder(name, op, res.map { (value: A) => (name, inject(value)) }))
  }

  override def apply[A](fa: ArgAlg.Op[A]): Const[ArgOptsBuilder, A] = fa match {
    case op@ArgAlg.BigintOp(name, description, defaultValue) =>
      option(name, op, description, defaultValue, BigIntValue)
    case op@ArgAlg.BoolOp(name, description, defaultValue) =>
      val res = defaultValue match {
        case None | Some(false) => Opts.flag(name, description).orFalse
        case Some(true) =>
          Opts
            .option[String](name, description, metavar = "boolean")
            .withDefault("true")
            .mapValidated {
              case "true" => Validated.valid(true)
              case "false" => Validated.valid(false)
              case other => Validated.invalidNel("Expecting `true' of `false'.")
            }
      }
      Const(ArgOptsBuilder(name, op, res.map { a => (name, BooleanValue(a)) }))
    case op@ArgAlg.IntOp(name, description, defaultValue) =>
      option(name, op, description, defaultValue, IntegerValue)
    case op@ArgAlg.StringOp(name, description, defaultValue) =>
      option(name, op, description, defaultValue, StringValue)
    case op@ArgAlg.TimestampOp(name, description, defaultValue) =>
      val opt = Opts
        .option[String](name, description, metavar = "timestamp")
        .mapValidated { str =>
          Try[ValidatedNel[String, Timestamp]] {
            Validated.valid[NonEmptyList[String], Timestamp](
              Timestamp.valueOf(
                LocalDateTime.parse(
                  str, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))))
          }.recover {
            case NonFatal(e) => Validated.invalidNel(e.toString)
          }.get
        }
      val res = defaultValue match {
        case Some(value) => opt.withDefault(value)
        case None => opt
      }
      Const(ArgOptsBuilder(name, op, res.map { a => (name, TimestampValue(a)) }))
  }
}

object ArgToOpts extends ArgToOpts

case class ArgOptsBuilder(argsAndOpts: Map[String, (ArgAlg.Op[_], Opts[(String, ParameterValue)])]) {
  def opts: Opts[Map[String, ParameterValue]] = argsAndOpts.values.map(_._2).toList.sequence.map(_.toMap)
}

object ArgOptsBuilder {
  def apply(name: String, arg: ArgAlg.Op[_], opts: Opts[(String, ParameterValue)]): ArgOptsBuilder = ArgOptsBuilder(Map(name -> ((arg, opts))))


  implicit object argOptsBuilderMonoid extends Monoid[ArgOptsBuilder] {
    override def empty: ArgOptsBuilder = ArgOptsBuilder(Map.empty)

    override def combine(x: ArgOptsBuilder, y: ArgOptsBuilder): ArgOptsBuilder = {
      val intersection = x.argsAndOpts.keySet.intersect(y.argsAndOpts.keySet)

      val different = intersection
        .map(name => (x.argsAndOpts(name)._1, y.argsAndOpts(name)._1))
        .filter { case (xa: ArgAlg.Op[_], ya: ArgAlg.Op[_]) => xa != ya }
      if (different.nonEmpty) throw new Exception(s"Found different arguments with same names: ${different.mkString(", ")}.")

      ArgOptsBuilder(x.argsAndOpts ++ y.argsAndOpts)
    }
  }

}