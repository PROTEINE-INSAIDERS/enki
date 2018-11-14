package enki.pm.project

import java.nio.file.Path

import enki.pm.fs.FileSystem

//TODO: remove fileSystem, decompose to project dirs.
case class ProjectFileSystem[F[_]](
                                    fileSystem: FileSystem[F],
                                    projectDir: Path
                                  ) {
  import ProjectFileSystem._

  def enkiDirLocation: Path = projectDir.resolve(enkiDir)

  def answerFileLocation: Path = enkiDirLocation.resolve(answerFile)
}

object ProjectFileSystem {
  val enkiDir = ".enki"
  val answerFile = "answers"
}