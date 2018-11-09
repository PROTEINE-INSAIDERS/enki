package enki.pm.cli

import java.nio.file.Path

import cats.data._

class PromptQuestions extends Prompt[Const[String, ?]] {
  override def projectName: Const[String, String] = Const("Project name:")

  override def sqlRoot: Const[String, String] = Const("Where sql files located?")

  override def projectDir: Const[String, Path] = Const("Enter project directory:")

  override def createNewProject(path: Path): Const[String, Boolean] = Const(s"Would you like to create a new project in `$path'?")
}
