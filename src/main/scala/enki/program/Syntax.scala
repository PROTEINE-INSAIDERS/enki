package enki
package program

import cats.free.FreeApplicative._
import cats.implicits._
import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag

trait Syntax {
  def read[T: TypeTag](src: Symbol)(implicit reader: Reader): Program[Dataset[T]] =
    lift[Statement, Dataset[T]](Read[T](src, reader))

  def session: Program[SparkSession] = lift(Session)

  def stage[T](id: Symbol, program: Program[Dataset[T]]): Program[Dataset[T]] =
    lift[Statement, Dataset[T]](Stage[T](id, program))

  def write[T: TypeTag](dst: Symbol)(implicit writer: Writer): Program[Dataset[T] => Unit] =
    lift[Statement, Dataset[T] => Unit](Write(dst, writer))

  def write[T: TypeTag](dst: Symbol, data: Program[Dataset[T]])(implicit writer: Writer): Program[Unit] = data.ap(write(dst))
}
