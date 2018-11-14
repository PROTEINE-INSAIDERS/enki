package enki.pm.cli

import java.nio.file.Path

trait Prompt[F[_]] {
  def scanDir: F[Boolean]

  def sqlRoot: F[String]
}
