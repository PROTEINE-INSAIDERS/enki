package enki.pm.project

import cats._
import cats.effect._
import cats.implicits._
import enki.pm.cli._
import enki.pm.fs.IODir
import io.chrisdavenport.log4cats.Logger

case class Project[F[_]](
                          name: F[String],
                          root: F[ModuleTree[F]]
                        )

object Project {
  cats.free.Free

  def cliProject[F[_] : Monad](implicit prompt: Prompt[F], io: LiftIO[F], logger: Logger[F]): Project[F] = Project[F](
    name = prompt.projectName,
    root = for {
      projectDir <- prompt.projectDir
      sqlRoot <- prompt.sqlRoot map { s => new IODir[F](new java.io.File(projectDir, s)) }
    } yield ModuleTree.fromDir(sqlRoot)
  )

  def build[F[_] : Monad](project: Project[F]): F[Project[Id]] = for {
    name <- project.name
    root <- project.root >>= (_.build)
  } yield Project[Id](name, root)
}