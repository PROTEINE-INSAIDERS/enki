package enki.pm.cli

import cats.data._

class PromptQuestions extends Prompt[Const[String, ?]] {
  override def projectName: Const[String, String] = Const("Enter project name:")

  override def sqlRoot: Const[String, String] = Const("Where sql files located?")

  override def whereDoYouWantToGoToday: Const[String, String] = Const("Where do you want to go today?™")
}
