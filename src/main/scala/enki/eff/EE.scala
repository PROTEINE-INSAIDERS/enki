package enki.eff

import cats._
import cats.implicits._
import org.apache.spark.sql.Dataset
import org.atnos.eff._
import interpret._
import cats.Traverse
import cats.data._
import cats.implicits._

import org.atnos.eff._
import org.atnos.eff.either._
import org.atnos.eff.writer._
import org.atnos.eff.state._
import org.atnos.eff.interpret._
import cats.implicits._
import cats.data._


import scala.collection.mutable._

sealed trait EE[+A]

case class Write[T](name: String, data: Dataset[T]) extends EE[Unit]

case class Read[T](name: String) extends EE[Dataset[T]]

object o {

  val aaa = (Some(1), Some(2), Some(3)).tupled

  import org.atnos.eff._

  // T |= R is an alias for MemberIn[T, R]
  // stating that effects of type T[_] can be injected in the effect stack R
  // It is also equivalent to MemberIn[EE, R]
  type _ee[R] = EE |= R

  def write[T, R: _ee](name: String, data: Dataset[T]): Eff[R, Unit] =
    Eff.send[EE, R, Unit](Write(name, data))

  def read[T, R :_ee](name: String): Eff[R, Dataset[T]] =
    Eff.send[EE, R, Dataset[T]](Read(name))



  def program[R :_ee]: Eff[R, Dataset[Int]] =
    for {
      _ <- write("wild-cats", null.asInstanceOf[Dataset[Int]])
      n <- read[Int, R]("wild-cats")
    } yield n


  def runKVStoreUnsafe[R, A](effects: Eff[R, A])(implicit m: EE <= R): Eff[m.Out, A] = {

    val sideEffect = new SideEffect[EE] {
      def apply[X](kv: EE[X]): X =
        kv match {
          case _ => ???
        }

      def applicative[X, Tr[_] : Traverse](ms: Tr[EE[X]]): Tr[X] =
        ms.map(apply)
    }

    interpretUnsafe(effects)(sideEffect)(m)

  }

  type _writerString[R] = Writer[String, ?] |= R
  type _stateMap[R]     = State[Map[String, Any], ?] |= R

  def runKVStore[R, U, A](effects: Eff[R, A])
                         (implicit m: Member.Aux[EE, R, U],
                          throwable:_throwableEither[U],
                          writer:_writerString[U],
                          state:_stateMap[U]): Eff[U, A] = {

    translate(effects)(new Translate[EE, U] {
      def apply[X](kv: EE[X]): Eff[U, X] =
        kv match {
          case Write(key, value) =>
            for {
              _ <- tell(s"put($key, $value)")
              // _ <- modify((map: Map[String, Any]) => map.updated(key, value))
              r <- fromEither(Either.catchNonFatal(().asInstanceOf[X]))
            } yield r

          case Read(key) =>
            for {
              _ <- tell(s"get($key)")
              // m <- get[U, Map[String, Any]]
              // r <- fromEither(Either.catchNonFatal(m.get(key).asInstanceOf[X]))
            } yield null.asInstanceOf[X]
        }
    })
  }
}