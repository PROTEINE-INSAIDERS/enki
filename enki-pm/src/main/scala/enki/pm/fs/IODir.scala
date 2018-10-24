package enki.pm.fs

import cats.effect._

class IODir[F[_]](file: java.io.File)(implicit io: LiftIO[F]) extends Dir[F] {
  override def contents: F[Seq[DirTree[F]]] = io.liftIO {
    IO {
      file.listFiles().map(f => if (f.isFile) {
        new IOFile[F]()
      } else {
        new IODir[F](f)
      })
    }
  }
}
