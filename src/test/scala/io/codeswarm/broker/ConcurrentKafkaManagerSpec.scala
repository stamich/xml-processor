package io.codeswarm.broker

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.codeswarm.api.{EmbProducer, EmbStreamingProducer}
import io.codeswarm.file.FileHandler
import org.mockito.{ArgumentCaptor, ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, File}
import java.util.Properties

class ConcurrentKafkaManagerSpec extends AnyFunSuite with MockitoSugar with ArgumentMatchersSugar with Matchers {

  test("produceRecords should process files concurrently") {
    // Setup mocks
    val producerManager = mock[ProducerManager]
    val standardProducer = mock[EmbProducer[String]]
    val bigReportProducer = mock[EmbStreamingProducer]
    val fileHandler = mock[FileHandler]

    // Create test files
    val smallFile = new File("small.xml")
    val largeFile = new File("large.xml")
    val files = List(smallFile, largeFile)

    // Mock properties and har path
    val props = new Properties()
    val harPath = "/test/archive.har"
    val maxConcurrent = 2

    // Mock file content
    val smallFileContent = "<root>  <node>small content</node>  </root>"
    val largeFileContent = "<root>" + "x" * 2000000 + "</root>"

    // Mock behavior
    when(producerManager.createReportProducer(any[Properties])).thenReturn(standardProducer)
    when(producerManager.createBigReportProducer(any[Properties])).thenReturn(bigReportProducer)
    when(fileHandler.listXmlFilesFromHar(eqTo(harPath))).thenReturn(IO.pure(files))
    when(fileHandler.readFileContentFromHar(eqTo(harPath), eqTo(smallFile))).thenReturn(IO.pure(smallFileContent))
    when(fileHandler.readFileContentFromHar(eqTo(harPath), eqTo(largeFile))).thenReturn(IO.pure(largeFileContent))
    when(producerManager.makeHeaders(any[String])).thenReturn(Map("content-type" -> "application/xml"))

    // Create SUT with injected mocks
    val concurrentManager = new ConcurrentKafkaManager {
      override private[broker] def createProducerManager() = producerManager
      override private[broker] def createFileHandler() = fileHandler
    }

    // Execute
    concurrentManager.produceRecords(props, harPath, maxConcurrent).unsafeRunSync()

    // Verify
    verify(fileHandler).listXmlFilesFromHar(harPath)
    verify(fileHandler).readFileContentFromHar(eqTo(harPath), eqTo(smallFile))
    verify(fileHandler).readFileContentFromHar(eqTo(harPath), eqTo(largeFile))

    // Verify the standard producer was called with the small file content
    verify(standardProducer).send(
      eqTo(smallFileContent),
      any[Map[String, String]]
    )

    // Verify the big producer was called with the large file content
    val streamingCaptor = ArgumentCaptor.forClass(classOf[ByteArrayInputStream])
    verify(bigReportProducer).send(
      streamingCaptor.capture(),
      any[Map[String, String]]
    )

    // Verify producers were closed
    verify(bigReportProducer).close()
    verify(standardProducer).close()
  }
}
