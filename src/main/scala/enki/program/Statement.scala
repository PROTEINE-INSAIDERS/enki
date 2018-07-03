package enki
package program

import cats._
import cats.implicits._
import enki.readers.Reader
import enki.writers.Writer
import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag

sealed abstract class Statement[T]

final case class Read[T: TypeTag, A](name: Symbol, reader: Reader, f: Dataset[T] => A) extends Statement[A] {
  val typeTag: TypeTag[T] = implicitly[TypeTag[T]]
}

final case class Write[T: TypeTag, A](name: Symbol, writer: Writer, program: Program[Dataset[T]], value: A) extends Statement[A] {
  val typeTag: TypeTag[T] = implicitly[TypeTag[T]]
}

trait StatementInstances {
  //TODO: Зачем нам тут функтор?
  implicit val statementInstances: Functor[Statement] = new Functor[Statement] {
    override def map[A, B](fa: Statement[A])(f: A => B): Statement[B] = fa match {
      case r@Read(name, reader, g) => Read(name, reader, f <<< g)(r.typeTag)
      case w@Write(name, writer, p, a) => Write(name, writer, p, f(a))(w.typeTag)
    }
  }
}