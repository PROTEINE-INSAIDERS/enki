package enki.pm.fs

import java.nio.charset._
import java.nio.file._

/**
  * File system algebra.
  */
trait FileSystem[F[_]] {
  def isDirectory(path: Path, options: LinkOption*): F[Boolean]

  def isRegularFile(path: Path, options: LinkOption*): F[Boolean]

  def list(path: Path): F[List[Path]]

  def readAllText(path: Path, charset: Charset): F[String]

  def createDirectories(path: Path): F[Path]
}

object FileSystem {
  def apply[F[_]](implicit ev: FileSystem[F]): FileSystem[F] = ev
}