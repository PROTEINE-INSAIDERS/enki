package enki.oozie.validator

object WorkflowValidator extends Validator {
  override def xsdPaths: Array[String] = Array(
    "enki/src/main/xsd/oozie-workflow-1.0.xsd",
    "enki/src/main/xsd/oozie-common-1.0.xsd",
    "enki/src/main/xsd/spark-action-1.0.xsd"
  )
}
