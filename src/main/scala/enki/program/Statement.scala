package enki
package program

import enki.readers.Reader
import enki.writers.Writer
import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag

sealed abstract class Statement[T]

object Session extends Statement[SparkSession]

case class Read[T: TypeTag](name: Symbol, source: Reader) extends Statement[Dataset[T]] {
  val typeTag: TypeTag[T] = implicitly[TypeTag[T]]
}

case class Stage[T](name: Symbol, program: Program[Dataset[T]]) extends Statement[Dataset[T]]

case class Write[T: TypeTag](name: Symbol, writer: Writer) extends Statement[Dataset[T] => Unit] {
  val typeTag: TypeTag[T] = implicitly[TypeTag[T]]
}