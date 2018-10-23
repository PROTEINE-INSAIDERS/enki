package enki.pm.project

final case class SqlFile(
                  fileName: String,
                  fileStage: SqlFileState
                  )

sealed trait SqlFileState

final case class ParsedSqlFile() //TODO: информация о таблицах, создаваемых в данном файле и таблицах, которые он читает.

final case class ExcludedSqlFile()