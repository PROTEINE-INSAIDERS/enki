import cats.free.FreeApplicative

package object enki {
  type Program[A] = FreeApplicative[program.Statement, A]

  type ReadSt[A] = program.Read[A]
  val ReadSt = program.Read

  val SessionSt = program.Session


  type Reader = readers.Reader
  type EmptyReader = readers.EmptyReader
  type SchemaFromSource = readers.SchemaFromSource

  type Writer = writers.Writer
}
