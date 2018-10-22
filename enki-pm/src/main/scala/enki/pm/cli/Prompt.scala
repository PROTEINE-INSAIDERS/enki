package enki.pm.cli

trait Prompt[F[_]] {
  def projectName: F[String]

  def whereDoYouWantToGoToday: F[String]
}
