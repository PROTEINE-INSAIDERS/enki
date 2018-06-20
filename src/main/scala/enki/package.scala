import cats.free.FreeApplicative

package object enki {
  type Statement[A] = program.Statement[A]
  type Program[A] = FreeApplicative[Statement, A]

  type Read[A] = program.Read[A]
  val Read = program.Read
  val Session = program.Session
  type Write[A] = program.Write[A]
  val Write = program.Write

  type Reader = readers.Reader
  type EmptyReader = readers.EmptyReader
  type TableReader = readers.TableReader
  type SchemaFromSource = readers.SchemaFromSource

  type Writer = writers.Writer
}
