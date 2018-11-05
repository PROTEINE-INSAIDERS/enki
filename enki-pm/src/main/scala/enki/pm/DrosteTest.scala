package enki.pm

import java.nio.file.Path

import cats._
import cats.implicits._
import enki.pm.fs._
import qq.droste
import qq.droste._
import qq.droste.data._

sealed trait SimpleTreeF[A]

case class TreeLeaf[A](data: String) extends SimpleTreeF[A]

case class TreeNode[A](l: A, r: A) extends SimpleTreeF[A]



object SimpleTreeF {
  implicit val simpleTreeFunctor: Functor[SimpleTreeF] = new Functor[SimpleTreeF] {
    override def map[A, B](fa: SimpleTreeF[A])(f: A => B): SimpleTreeF[B] = fa match {
      case TreeLeaf(data) => TreeLeaf(data)
      case TreeNode(l, r) => TreeNode(f(l), f(r))
    }
  }
}

sealed trait FixedTree
case class FixedTreeLeaf(data: String) extends FixedTree
case class FixedTreeNode(l: FixedTree, r: FixedTree) extends FixedTree

object FixedTree {
  implicit val treeBasis = Basis.Default(
    Algebra[SimpleTreeF, FixedTree] {
      case TreeLeaf(name) => FixedTreeLeaf(name)
      case TreeNode(l, r) => FixedTreeNode(l, r)
    },
    Coalgebra[SimpleTreeF, FixedTree] {
      case FixedTreeLeaf(name) => TreeLeaf(name)
      case FixedTreeNode(l, r) => TreeNode(l, r)
    }
  )
}

object DrosteTest {
  def testCata(): Unit = {
    import FixedTree._

    val a = Algebra[SimpleTreeF, String] {
      case TreeLeaf(name) => name
      case TreeNode(l, r) => l + " " + r
    }
    scheme.cata[SimpleTreeF, FixedTree, String](a).apply(FixedTreeNode(FixedTreeLeaf("a"), FixedTreeLeaf("b")))
  }

  def testAnaCata(): Unit = {
    val coAlg = Coalgebra[SimpleTreeF, Int] {
      case 0 => TreeLeaf("stop")
      case i => TreeNode(i - 1, i - 1)
    }
    val a = scheme.ana(coAlg).apply(2)
    println(a)
    val alg = Algebra[SimpleTreeF, String] {
      case TreeLeaf(str) => str
      case TreeNode(l, r) => l + " " + r
    }
    val b = scheme.cata(alg).apply(a)
    println(b)
  }

  def testApoPara(): Unit = {
    val coAlg = RCoalgebra[Fix[SimpleTreeF], SimpleTreeF, Int] {
      case 0 => TreeLeaf("stop")
      case i => TreeNode(Right(i - 1), Left(Fix[SimpleTreeF](TreeLeaf("earlyTerm"))))
    }
    val a: Fix[SimpleTreeF] = scheme.zoo.apo(coAlg).apply(2)
    println(a)
    val alg = RAlgebra[Fix[SimpleTreeF], SimpleTreeF, String] {
      case TreeLeaf(str) => str
      case TreeNode((TreeLeaf("stop"), _), (_, r)) => "left fix replaced " + r
      case TreeNode((_, l), (_, r)) => l + " " + r
    }
    val b = scheme.zoo.para(alg).apply(a)
    println(b)
  }

  def main(args: Array[String]): Unit = {
    testApoPara()

    /*
    val root = Paths.get(System.getProperty("user.home"), "Projects/test-enki-project")

    implicit val fs = new NioFileSystem[IO]()

    val test1 = RCoalgebra[Int, List, String] { aa =>
      List(Right[Int, String](aa), Left[Int, String](10))
    }

    println(scheme.zoo.apo(test1).apply("start"))


    val aaa: CoalgebraM[IO, CoattrF[List, Path, ?], Path] = COA.files[IO]

    val arrr = drosteArrowForGCoalgebra[IO]


    //    val test = aaa.run(???)

    val res = scheme.anaM(aaa).apply(root).unsafeRunSync()

    println(res)
    */
  }
}
