package enki.pm.cli

sealed trait Question[A]{
  def questionStr: String
}

final case class WhereDoYouWantToGoToday() extends Question[String] {
  override def questionStr: String =  "Where do you want to go today?"
}

final case class ProjectName() extends Question[String] {
  override def questionStr: String = "Enter project name:"
}