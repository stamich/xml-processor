package io.codeswarm.validator

import java.io.File
import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.{SchemaFactory, Validator}
import scala.util.Try

class XmlValidator {

  def validateXml(xmlFile: File, xsdFile: File): Try[Boolean] = Try {
    val factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
    val schema = factory.newSchema(xsdFile)
    val validator: Validator = schema.newValidator()
    validator.validate(new StreamSource(xmlFile))
    true
  }.recover{
    case ex => println(s"Validation error in file ${xmlFile.getName}: ${ex.getMessage}")
      false
  }
}
