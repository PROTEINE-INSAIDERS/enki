package enki.pm.project

import cats._
import cats.implicits._

case class Project[F[_]](name: F[String], whereToGo: F[String])

object Project {
  def cliProject[F[_]]: Project[F] = Project[F](
    name = ???,
    whereToGo = ???
  )

  def build[F[_]: Applicative](project: Project[F]): F[Project[Id]] = {
    (project.name, project.whereToGo) mapN Project[Id]
  }
}