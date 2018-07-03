package enki
package program

import cats.free.FreeApplicative._
import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag

trait Syntax {
  def read[T: TypeTag](src: Symbol)(implicit reader: Reader): Program[Dataset[T]] =
    lift[Statement, Dataset[T]](Read[T, Dataset[T]](src, reader, identity))

  def write[T: TypeTag](table: Symbol, data: Program[Dataset[T]])(implicit writer: Writer): Program[Unit] =
    lift[Statement, Unit](Write(table, writer, data, Unit))
}
