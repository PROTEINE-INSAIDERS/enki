package enki.spark.sql

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

  private def getColumnName[A: WeakTypeTag, B: WeakTypeTag](selector: Expr[A => B]): String = {
    selector.tree match {
      case NameExtractor(str) => str
      case Function(_, body) => colSelectorFailed(body)
      // $COVERAGE-OFF$ - cannot be reached as typechecking will fail in this case before macro is even invoked
      case other => colSelectorFailed(other)
      // $COVERAGE-ON$
    }
  }

  //TODO: от неё никакого толка. Обычно колонки удаляются после withColumn, либо после join-ов, когда тип датасета уже потерян.
  def column[A: WeakTypeTag, B: WeakTypeTag, R: WeakTypeTag](selector: Expr[A => B])
                                                            (relation: Expr[ColumnTypeMapping[A, R]],
                                                             encoder: Expr[Encoder[R]]): Expr[TypedColumn[A, R]] =
    c.Expr(q"${c.prefix}.dataset.col(${getColumnName(selector)}).as[${weakTypeOf[R].dealias}]($encoder)")

  def columnName[A, B](selector: Expr[A => B]): Expr[String] =
    c.Expr(q"${getColumnName(selector)}")

  def drop[A, B](selector: Expr[A => B]): Expr[DataFrame] =
    c.Expr(q"${c.prefix}.dataset.drop(${getColumnName(selector)})")

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
