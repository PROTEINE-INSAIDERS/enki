package enki.staging

sealed trait Counter[+A]

case class Inc() extends Counter[Int]

import cats.data._
import org.atnos.eff._
import org.atnos.eff.interpret._
import org.atnos.eff.state._
import org.atnos.eff._, syntax.all._
import cats._, data._

object ooo {

  type _counter[R] = Counter |= R
  type _stateCounter[R] = State[Int, ?] |= R

  def inc[R: _counter]: Eff[R, Int] = Eff.send[Counter, R, Int](Inc())

  def program[R: _counter]: Eff[R, Int] = for {
    a <- inc
  } yield a

  def runCounter[R, U, A](effects: Eff[R, A])(implicit m: Member.Aux[Counter, R, U],
                                              state: _stateCounter[U]): Eff[U, A] = {
    translate(effects)(new Translate[Counter, U] {
      override def apply[X](kv: Counter[X]): Eff[U, X] =
        kv match {
          case Inc() => for {
            a <- get
            _ <- put(a + 12)
          } yield a
        }
    })
  }

  def main(args: Array[String]): Unit = {
    type Stack = Fx.fx2[Counter, State[Int, ?]]


    val aa = runCounter(program[Stack]).runState(0).run

    println(aa)
  }
}