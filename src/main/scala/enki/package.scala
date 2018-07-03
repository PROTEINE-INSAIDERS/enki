package object enki {
  type Program[A] = program.Program[A]
  type Statement[A] = program.Statement[A]

  type Read[T, A] = program.Read[T, A]
  val Read = program.Read
  type Write[T, A] = program.Write[T, A]
  val Write = program.Write

  type Reader = readers.Reader
  type EmptyReader = readers.EmptyReader
  type TableReader = readers.TableReader
  type SchemaFromSource = readers.SchemaFromSource

  type Writer = writers.Writer
  type EmptyWriter = writers.Writer
  type TableWriter = writers.TableWriter
}
