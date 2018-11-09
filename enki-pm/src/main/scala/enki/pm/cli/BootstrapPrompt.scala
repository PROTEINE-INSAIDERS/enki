package enki.pm.cli

import java.nio.file._

trait BootstrapPrompt[F[_]] {
  def createNewProject(path: Path): F[Boolean]

  def projectDir: F[Path]
}

abstract class ConsoleBootstrapPrompt[F[_]] extends BootstrapPrompt[F] {

}