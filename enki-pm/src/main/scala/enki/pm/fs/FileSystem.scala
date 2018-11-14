package enki.pm.fs

import java.nio.charset._
import java.nio.file._

trait FileSystem[F[_]] {
  def createDirectories(path: Path): F[Path]

  def isDirectory(path: Path, options: LinkOption*): F[Boolean]

  def isRegularFile(path: Path, options: LinkOption*): F[Boolean]

  def list(path: Path): F[List[Path]]

  def readAllText(path: Path, charset: Charset = Charset.defaultCharset()): F[String]

  def writeAllText(path: Path, text: String, charset: Charset = Charset.defaultCharset()): F[Unit]
}

object FileSystem {
  def apply[F[_]](implicit ev: FileSystem[F]): FileSystem[F] = ev
}