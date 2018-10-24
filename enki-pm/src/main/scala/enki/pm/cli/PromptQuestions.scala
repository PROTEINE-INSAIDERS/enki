package enki.pm.cli

import cats.data._

class PromptQuestions extends Prompt[Const[String, ?]] {
  override def projectName: Const[String, String] = Const("Project name:")

  override def sqlRoot: Const[String, String] = Const("Where sql files located?")

  override def projectDir: Const[String, String] = Const("Project folder:")
}
