package enki.pm.fs

import java.nio.file.{LinkOption, Path}

/**
  * File system algebra.
  */
trait FileSystem[F[_]] {
  def isDirectory(path: Path, options: LinkOption*): F[Boolean]

  def list(path: Path): F[List[Path]]

  def isRegularFile(path: Path, options: LinkOption*): F[Boolean]
}
