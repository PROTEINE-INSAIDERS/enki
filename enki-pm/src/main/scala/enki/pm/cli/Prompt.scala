package enki.pm.cli

import java.nio.file.Path

trait Prompt[F[_]] {
  def projectName: F[String]

  def sqlRoot: F[String]

  def projectDir: F[Path]
}
