package enki.pm.cli

import java.nio.file.Path

trait Prompt[F[_]] {
  def createNewProject(path: Path): F[Boolean]

  def projectName: F[String]

  def sqlRoot: F[String]

  def projectDir: F[Path]
}
