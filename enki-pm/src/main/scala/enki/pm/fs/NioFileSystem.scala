package enki.pm.fs

import java.nio.charset._
import java.nio.file._

import cats.effect._

import scala.collection.JavaConverters._

/**
  * Java NIO - based file system implementation.
  */
case class NioFileSystem[F[_]](implicit liftIO: LiftIO[F]) extends FileSystem[F] {
  private def io[A](body: => A): F[A] = liftIO.liftIO(IO(body))

  override def isDirectory(path: Path, options: LinkOption*): F[Boolean] = io {
    Files.isDirectory(path, options: _*)
  }

  override def isRegularFile(path: Path, options: LinkOption*): F[Boolean] = io {
    Files.isRegularFile(path, options: _*)
  }

  override def list(path: Path): F[List[Path]] = io {
    Files.list(path).iterator().asScala.toList
  }

  override def readAllText(path: Path, charset: Charset): F[String] = io {
    new String(Files.readAllBytes(path), charset)
  }

  override def createDirectories(path: Path): F[Path] = io {
    Files.createDirectories(path)
  }

  override def writeAllText(path: Path, text: String, charset: Charset): F[Unit] = io {
    Files.write(path, text.getBytes(charset))
    ()
  }
}
