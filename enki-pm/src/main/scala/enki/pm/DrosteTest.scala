package enki.pm

import java.nio.file.Files

import cats.effect.IO
import cats.implicits._
import qq.droste._
import qq.droste.implicits._

object DrosteTest {
  def main(args: Array[String]): Unit = {
    /*
        val natCoalgebra: Coalgebra[Option, BigDecimal] =
          Coalgebra(n => if (n > 0) Some(n - 1) else None)

        val fibAlgebra: CVAlgebra[Option, BigDecimal] = CVAlgebra {
          case Some(r1 :< Some(r2 :< _)) => r1 + r2
          case Some(_ :< None) => 1
          case None => 0
        }

        val fib: BigDecimal => BigDecimal = scheme.ghylo(
          fibAlgebra.gather(Gather.histo),
          natCoalgebra.scatter(Scatter.ana))

        val a = fib(100)

        println(a)

        val natCoalgebraM: CoalgebraM[IO, Option, Int] = CoalgebraM(n => for {
          _ <- IO {
            println(s"Unfold: $n")
          }
        } yield if (n > 0) Some(n - 1) else None)

        val rr = scheme.anaM(natCoalgebraM).apply(5).unsafeRunSync()

        println(rr)
    */

    val fileCoalgebraM: CoalgebraM[IO, List, java.io.File] = CoalgebraM(dir =>
      for {
        _ <- IO {
          println(s"${dir.getAbsolutePath}")
        }
        filez <- IO {
          val files = dir.listFiles()
          if (files == null) throw new Exception(s"${dir.getAbsolutePath} listFiles returned null")
          files.toList.filter(f => f.isDirectory && !(Files.isSymbolicLink(f.toPath)))
        }
      } yield filez)

    val testt = scheme.anaM(fileCoalgebraM) // IO[Fix[List]]

    val listSumCoalgebra: Algebra[List, Int] = Algebra { l => if (l.isEmpty) 1 else l.sum }

    val yoba = scheme.hyloM( // какая схема позволяет видеть объекты из анаморфизма
      listSumCoalgebra.lift[IO], fileCoalgebraM
    )

    { yoba(new java.io.File(System.getProperty("user.home"))) >>= (f => IO { println(f) }) }.unsafeRunSync()
  }
}
