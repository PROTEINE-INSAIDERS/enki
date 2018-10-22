package enki.pm.cli

import atto._
import atto.Atto._
import cats.data._

class PromptParsers extends Prompt[Lambda[a => Const[Parser[a], a]]] {
 override def sqlRoot: Const[Parser[String], String] = Const(takeText)

  override def projectName: Const[Parser[String], String] = Const(takeText)

  override def whereDoYouWantToGoToday: Const[Parser[String], String] = Const(string("Microsoft®"))
}
