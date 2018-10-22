package enki.pm.project

import cats._
import cats.implicits._
import enki.pm.cli._

case class Project[F[_]](
                          name: F[String],
                          whereToGo: F[String], //Just for testing.
                          sqlRoot: F[SqlModule]
                        )

//TODO: собрать информацию об испльзуюемых sql модулях.

object Project {
  def cliProject[F[_]: Monad](implicit p: Prompt[F]): Project[F] = Project[F](
    name = p.projectName,
    whereToGo = p.whereDoYouWantToGoToday,
    sqlRoot = p.sqlRoot >>= { sqlRoot =>
      //TODO: нужен класс для чтения sql файлов + класс для доступа к файловой системе (собственно, класс для чтения
      // sql файлов инкапсулирует класс для доступа к файловой системе).
      ???
    }
  )

  def build[F[_] : Applicative](project: Project[F]): F[Project[Id]] = {
    (project.name, project.whereToGo, project.sqlRoot) mapN Project[Id]
  }
}