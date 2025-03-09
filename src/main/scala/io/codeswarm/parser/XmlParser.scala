package io.codeswarm.parser

import org.w3c.dom.{Node, NodeList}

import java.io.File
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult
import javax.xml.transform.{OutputKeys, TransformerFactory}
import scala.util.Try

class XmlParser {

  def removeWhitespace(inputFile: File, outputFile: File): Try[Unit] = Try {
   val factory = DocumentBuilderFactory.newInstance()
   factory.setIgnoringElementContentWhitespace(true)
   val builder = factory.newDocumentBuilder()
   val document = builder.parse(inputFile)

    document.getDocumentElement.normalize()
    removeWhitespaceNodes(document.getDocumentElement)

    val transformerFactory = TransformerFactory.newInstance()
    val transformer = transformerFactory.newTransformer()
    transformer.setOutputProperty(OutputKeys.INDENT, "yes")
    val source = new DOMSource(document)
    val result = new StreamResult(outputFile)
    transformer.transform(source, result)
  }

  def removeWhitespaceNodes(node: Node): Unit = {
    val children: NodeList = node.getChildNodes
    (0 until children.getLength)
      .map(children.item)
      .filter(child => child.getNodeType == Node.TEXT_NODE && child.getNodeValue.trim.isEmpty)
      .foreach(removeWhitespaceNodes)
  }
}
