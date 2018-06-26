package enki
package compiler

import cats._
import enki.program._
import org.apache.spark.sql._

trait Compiler {
  def readerMapper(f: Reader => Reader): Statement ~> Statement = λ[Statement ~> Statement] {
    case read@Read(name, reader, g) => Read(name, f(reader), g)(read.typeTag)
    case other => other // это приведет к пропуску stage
  }

  def writerMapper(f: Writer => Writer): Statement ~> Statement = λ[Statement ~> Statement] {
    case write@Write(n, w, p, a) => Write(n, f(w), p.compile(writerMapper(f)), a)(write.typeTag)
    case other => other // это приведет к пропуску stage
  }

  def evaluator(implicit session: SparkSession): Statement ~> Id = λ[Statement ~> Id] {
    case r@Read(name, reader, f) =>
      f(reader.read(name, session)(r.typeTag))
    case Session(f) =>
      f(session)
    case write@Write(n, w, p, a) =>
      val d = p.foldMap(evaluator(session))
      w.write(n, d, session)(write.typeTag)
      a
  }

  def eval[T](plan: Program[T])(implicit session: SparkSession): T = {
    plan.foldMap(evaluator(session))
  }
}
