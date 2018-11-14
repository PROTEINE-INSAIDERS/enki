package enki.pm.project

import java.nio.file.Path

import enki.pm.fs.FileSystem

case class ProjectFileSystem[F[_]](
                                    fileSystem: FileSystem[F],
                                    projectDir: Path
                                  )