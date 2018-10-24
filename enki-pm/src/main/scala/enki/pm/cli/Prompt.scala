package enki.pm.cli

trait Prompt[F[_]] {
  def projectName: F[String]

  def sqlRoot: F[String]

  def projectDir: F[String]
}
