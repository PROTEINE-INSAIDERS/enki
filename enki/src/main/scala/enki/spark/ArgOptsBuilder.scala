package enki.spark

import cats._
import cats.implicits._
import com.monovore.decline._

object ArgToOpts extends CollectArgs({ arg =>
  val opts = Opts.option[String](long = arg.name, help = "").map(value => (arg.name, value))
  ArgOptsBuilder(arg, opts)
})(ArgOptsBuilder.argOptsBuilderMonoid)

// сейчас сохраняется исходный аргумент, потому что в последствии у них могут появиться типы и описания и в моноиде надо
// будет их правильно комбинировать.
case class ArgOptsBuilder(argsAndOpts: Map[String, (Argument, Opts[(String, String)])]) {
  def opts: Opts[Map[String, String]] = argsAndOpts.values.map(_._2).toList.sequence.map(_.toMap)
}

object ArgOptsBuilder {
  def apply(arg: Argument, opts: Opts[(String, String)]): ArgOptsBuilder = ArgOptsBuilder(Map(arg.name -> ((arg, opts))))

  implicit object argOptsBuilderMonoid extends Monoid[ArgOptsBuilder] {
    override def empty: ArgOptsBuilder = ArgOptsBuilder(Map.empty)

    override def combine(x: ArgOptsBuilder, y: ArgOptsBuilder): ArgOptsBuilder = {
      ArgOptsBuilder(x.argsAndOpts ++ y.argsAndOpts)
    }
  }
}