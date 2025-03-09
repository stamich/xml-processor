package io.codeswarm.parser

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.w3c.dom.{Document, Node, NodeList}

import java.io.{ByteArrayInputStream, StringWriter}
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult
import javax.xml.transform.{OutputKeys, TransformerFactory}

class XmlParserSpec extends AnyFunSuite with Matchers {

  private val xmlParser = new XmlParser()

  private def parseXmlString(xml: String): Document = {
    val factory = DocumentBuilderFactory.newInstance()
    factory.setIgnoringElementContentWhitespace(true)
    val builder = factory.newDocumentBuilder()
    builder.parse(new ByteArrayInputStream(xml.getBytes))
  }

  private def nodeToString(node: Node): String = {
    val transformerFactory = TransformerFactory.newInstance()
    val transformer = transformerFactory.newTransformer()
    transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes")
    transformer.setOutputProperty(OutputKeys.INDENT, "no")

    val writer = new StringWriter()
    transformer.transform(new DOMSource(node), new StreamResult(writer))
    writer.toString
  }

  test("removeWhitespaceNodes should remove basic whitespace") {
    val xml = "<root>  <child>value</child>  </root>"
    val doc = parseXmlString(xml)

    xmlParser.removeWhitespaceNodes(doc.getDocumentElement)

    val result = nodeToString(doc)
    result shouldBe "<root><child>value</child></root>"
  }

  test("removeWhitespaceNodes should handle nested elements with whitespace") {
    val xml = "<root>\n  <parent>  \n  <child>  value  </child>\n  </parent>\n</root>"
    val doc = parseXmlString(xml)

    xmlParser.removeWhitespaceNodes(doc.getDocumentElement)

    val result = nodeToString(doc)
    result shouldBe "<root><parent><child>  value  </child></parent></root>"
    // Note: internal whitespace in text content is preserved
  }

  test("removeWhitespaceNodes should preserve attributes") {
    val xml = "<root attr=\"value\">  <child id=\"1\">text</child>  </root>"
    val doc = parseXmlString(xml)

    xmlParser.removeWhitespaceNodes(doc.getDocumentElement)

    val result = nodeToString(doc)
    result shouldBe "<root attr=\"value\"><child id=\"1\">text</child></root>"
  }

  test("removeWhitespaceNodes should handle mixed content") {
    val xml = "<root>text1  <child>value</child>  text2</root>"
    val doc = parseXmlString(xml)

    xmlParser.removeWhitespaceNodes(doc.getDocumentElement)

    val result = nodeToString(doc)
    result shouldBe "<root>text1  <child>value</child>  text2</root>"
    // Note: text nodes with non-whitespace content are preserved
  }

  test("removeWhitespaceNodes should handle empty elements") {
    val xml = "<root>  <empty/>  <child></child>  </root>"
    val doc = parseXmlString(xml)

    xmlParser.removeWhitespaceNodes(doc.getDocumentElement)

    val result = nodeToString(doc)
    result shouldBe "<root><empty/><child/></root>"
  }

  test("removeWhitespaceNodes should handle complex XML with CDATA sections") {
    val xml =
      """<root>
        |  <data>
        |    <![CDATA[
        |      This is CDATA content
        |      with preserved whitespace
        |    ]]>
        |  </data>
        |</root>""".stripMargin

    val doc = parseXmlString(xml)

    xmlParser.removeWhitespaceNodes(doc.getDocumentElement)

    val result = nodeToString(doc)
    // CDATA content is preserved, but whitespace between elements is removed
    result should include("<![CDATA[")
    result should include("This is CDATA content")
    result should not include(">  <")
  }

  test("removeWhitespaceNodes should handle XML with processing instructions") {
    val xml =
      """<?xml version="1.0" encoding="UTF-8"?>
        |<?xml-stylesheet type="text/xsl" href="style.xsl"?>
        |<root>
        |  <child>value</child>
        |</root>""".stripMargin

    val doc = parseXmlString(xml)

    xmlParser.removeWhitespaceNodes(doc.getDocumentElement)

    val result = nodeToString(doc)
    result shouldBe "<root><child>value</child></root>"
  }
}