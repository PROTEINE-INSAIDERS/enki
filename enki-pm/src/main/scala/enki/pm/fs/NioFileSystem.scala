package enki.pm.fs

import java.nio.file.{LinkOption, Path, _}

import cats.effect._
import scala.collection.JavaConverters._

/**
  * Java NIO - based file system implementation.
  */
class NioFileSystem[F[_]](implicit liftIO: LiftIO[F]) extends FileSystem[F] {
  private def io[A](body: => A): F[A] = liftIO.liftIO(IO(body))

  override def isDirectory(path: Path, options: LinkOption*): F[Boolean] = io {
    Files.isDirectory(path, options: _*)
  }

  override def list(path: Path): F[List[Path]] = io {
    Files.list(path).iterator().asScala.toList
  }

  override def isRegularFile(path: Path, options: LinkOption*): F[Boolean] = io {
    Files.isRegularFile(path, options: _*)
  }
}
