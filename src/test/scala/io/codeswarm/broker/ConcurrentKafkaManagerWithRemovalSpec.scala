package io.codeswarm.broker

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.codeswarm.api.{EmbProducer, EmbStreamingProducer}
import io.codeswarm.file.FileHandler
import io.codeswarm.parser.XmlParser
import org.mockito.{ArgumentCaptor, ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.w3c.dom.Element

import java.io.{ByteArrayInputStream, File}
import java.util.Properties
import javax.xml.parsers.DocumentBuilderFactory

class ConcurrentKafkaManagerWithRemovalSpec extends AnyFunSuite with MockitoSugar with ArgumentMatchersSugar with Matchers {

  test("produceRecords should process files with whitespace removal") {
    // Setup mocks
    val producerManager = mock[ProducerManager]
    val standardProducer = mock[EmbProducer[String]]
    val bigReportProducer = mock[EmbStreamingProducer]
    val fileHandler = mock[FileHandler]
    val xmlParser = mock[XmlParser]

    // Create test files
    val smallFile = new File("small.xml")
    val largeFile = new File("large.xml")
    val files = List(smallFile, largeFile)

    // XML content with whitespace
    val xmlContent = "<root>  <child>  value  </child>  </root>"

    // Mock properties
    val props = new Properties()
    val harPath = "/test/archive.har"

    // Mock behavior for DOM processing
    doNothing.when(xmlParser).removeWhitespaceNodes(any[Element])

    // Mock behavior
    when(producerManager.createReportProducer(any[Properties])).thenReturn(standardProducer)
    when(producerManager.createBigReportProducer(any[Properties])).thenReturn(bigReportProducer)
    when(fileHandler.listXmlFilesFromHar(eqTo(harPath))).thenReturn(IO.pure(files))
    when(fileHandler.readFileContentFromHar(eqTo(harPath), any[File])).thenReturn(IO.pure(xmlContent))
    when(producerManager.makeHeaders(any[String])).thenReturn(Map.empty[String, String])

    // Create SUT with mocks
    val concurrentManager = new ConcurrentKafkaManagerWithRemoval {
      override private[broker] def createProducerManager() = producerManager
      override private[broker] def createFileHandler() = fileHandler
      override private[broker] def createXmlParser() = xmlParser
    }

    // Execute
    concurrentManager.produceRecords(props, harPath, 2).unsafeRunSync()

    // Verify
    verify(fileHandler).listXmlFilesFromHar(harPath)
    verify(fileHandler, times(2)).readFileContentFromHar(eqTo(harPath), any[File])

    // Verify the XML was processed
    verify(xmlParser, times(2)).removeWhitespaceNodes(any[Element])

    // Verify producers were closed
    verify(bigReportProducer).close()
    verify(standardProducer).close()
  }
}