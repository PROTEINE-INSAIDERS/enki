package enki.pm.cli

import atto._
import atto.Atto._
import cats.data._

class PromptParsers extends Prompt[Lambda[a => Const[Parser[a], a]]] {
  override def whereDoYouWantToGoToday: Const[Parser[String], String] = Const(string(""))

  override def projectName: Const[Parser[String], String] = ???
}
