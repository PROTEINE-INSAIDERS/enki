package enki.oozie.validator

import javax.xml.transform.Source
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory

import scala.util.{Failure, Success, Try}

trait Validator {
  def xsdPaths: Array[String]

  def validate(xmlPath: String): Boolean =
    Try({
      val schemaLang = "http://www.w3.org/2001/XMLSchema"
      val factory = SchemaFactory.newInstance(schemaLang)
      val schema = factory.newSchema(
        xsdPaths.map(path => new StreamSource(path).asInstanceOf[Source])
      )
      val validator = schema.newValidator()
      validator.validate(new StreamSource(xmlPath))
    }) match {
      case Success(_) => true
      case Failure(exception) =>
        exception.printStackTrace()
        false
    }
}