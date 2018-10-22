package enki.pm.project

import cats._
import cats.implicits._
import enki.pm.cli._

case class Project[F[_]](name: F[String], whereToGo: F[String])

object Project {
  def cliProject[F[_]](implicit prompt: Prompt[F]): Project[F] = Project[F](
    name = ???,
    whereToGo = prompt.whereDoYouWantToGoToday
  )

  def build[F[_]: Applicative](project: Project[F]): F[Project[Id]] = {
    (project.name, project.whereToGo) mapN Project[Id]
  }
}