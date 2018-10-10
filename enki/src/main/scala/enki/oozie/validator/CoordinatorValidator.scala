package enki.oozie.validator

object CoordinatorValidator extends Validator {
  override def xsdPaths: Array[String] = Array(
    "enki/src/main/xsd/oozie-coordinator-0.5.xsd"
  )
}
