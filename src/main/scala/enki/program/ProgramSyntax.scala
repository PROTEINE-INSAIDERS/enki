package enki
package program

import cats._
import cats.free.FreeApplicative._
import enki.internal.DefaultsTo
import enki.internal.DefaultsTo._
import enki.sources.Source
import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag

trait ProgramSyntax {
  def session: Program[SparkSession] = lift(SessionSt)

  def source[T: TypeTag](name: Symbol)(implicit source: Source): Program[Dataset[T]] =
    lift[Statement, Dataset[T]](SourceSt[T](name, source))

  //TODO: возможно стоит переделать так, чтобы работало без ap
  def stage[T](name: Symbol)(program: Program[Dataset[T]]): Program[Dataset[T]] = lift[Statement, Dataset[T]](StageSt[T](name, program))
}
