import cats.free.FreeApplicative

package object enki {
  type Program[A] = FreeApplicative[program.Statement, A]

  type SourceSt[A] = program.SourceSt[A]
  val SourceSt = program.SourceSt

  type EmptySource = sources.EmptySource
  type SchemaFromResource = sources.SchemaFromResource
}
