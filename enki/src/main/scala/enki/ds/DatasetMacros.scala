package enki.ds

import org.apache.spark.sql._

import scala.collection.immutable.Queue
import scala.reflect.macros.whitebox

private class DatasetMacros(val c: whitebox.Context) {

  import c.universe._

  private def colSelectorFailed(tree: Tree): Nothing = {
    val err =
      s"Could not create a column identifier from $tree - try using _.a.b syntax"
    c.abort(tree.pos, err)
  }

  def column[A: WeakTypeTag, B: WeakTypeTag, R: WeakTypeTag](selector: Expr[A => B])
                                                            (relation: Expr[ColumnTypeMapping[A, R]],
                                                             encoder: Expr[Encoder[R]]): Expr[TypedColumn[A, R]] = {
    val R = weakTypeOf[R].dealias

    val selectorStr = selector.tree match {
      case NameExtractor(str) => str
      case Function(_, body) => colSelectorFailed(body)
      // $COVERAGE-OFF$ - cannot be reached as typechecking will fail in this case before macro is even invoked
      case other => colSelectorFailed(other)
      // $COVERAGE-ON$
    }

    c.Expr(q"${c.prefix}.dataset.col($selectorStr).as[$R]($encoder)")
  }

  def columnName[A, B](selector: Expr[A => B]): Expr[String] = {
    def fail(tree: Tree) = {
      val err =
        s"Could not create a column identifier from $tree - try using _.a.b syntax"
      c.abort(tree.pos, err)
    }

    val selectorStr = selector.tree match {
      case NameExtractor(str) => str
      case Function(_, body) => fail(body)
      // $COVERAGE-OFF$ - cannot be reached as typechecking will fail in this case before macro is even invoked
      case other => fail(other)
      // $COVERAGE-ON$
    }
    c.Expr(q"$selectorStr")
  }


  case class NameExtractor(name: TermName) {
    Self =>
    def unapply(tree: Tree): Option[Queue[String]] = tree match {
      case Ident(`name`) => Some(Queue.empty)
      case Select(Self(strs), nested) => Some(strs enqueue nested.toString)
      // $COVERAGE-OFF$ - Not sure if this case can ever come up and Encoder will still work
      case Apply(Self(strs), List()) => Some(strs)
      // $COVERAGE-ON$
      case _ => None
    }
  }

  object NameExtractor {
    def unapply(tree: Tree): Option[String] = tree match {
      case Function(List(ValDef(_, name, _, _)), body) => NameExtractor(name).unapply(body).map(_.mkString("."))
      case _ => None
    }
  }

}
