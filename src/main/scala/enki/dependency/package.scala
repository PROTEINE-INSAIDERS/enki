package enki

import scalax.collection.Graph // or scalax.collection.mutable.Graph
import scalax.collection.GraphPredef._, scalax.collection.GraphEdge._

trait Test

package object dependency {
  def test() = {
    val a = new Test {}
    val b = new Test {}

    val c = a ~> b

    val grap = Graph(c)


  }

}
