package enki.tmp

import cats.data.Const
import cats.implicits._
import freestyle.free._
import freestyle.free.implicits._
import org.apache.spark.sql._

@free trait FreeDataFrameWriter {
  def mode(saveMode: SaveMode): FS[Unit]

  def format(source: String): FS[Unit]
}

object Test {

  def p[F[_]](implicit F: FreeDataFrameWriter[F]): F.FS[Unit] = {
    import F._
    mode(SaveMode.Overwrite) *> format("sql")
  }

  def main(args: Array[String]): Unit = {
    implicit val handler = new FreeDataFrameWriter.Handler[Const[List[String], ?]] {
      override protected[this] def mode(saveMode: SaveMode): Const[List[String], Unit] = Const(List(s"mode($saveMode)"))

      override protected[this] def format(source: String): Const[List[String], Unit] = Const(List(s"source($source)"))
    }

    val a = p[FreeDataFrameWriter.Op].interpret[Const[List[String], ?]]
    println(a.getConst)
  }
}
