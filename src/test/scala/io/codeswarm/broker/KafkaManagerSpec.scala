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

class KafkaManagerSpec extends AnyFunSuite with MockitoSugar with ArgumentMatchersSugar with Matchers {

  test("produceRecords should handle small and large files correctly") {
    // Setup mocks
    val producerManager = mock[ProducerManager]
    val standardProducer = mock[EmbProducer[String]]
    val bigReportProducer = mock[EmbStreamingProducer]
    val fileHandler = mock[FileHandler]

    // Create test files
    val smallFile = new File("small.xml")
    val largeFile = new File("large.xml")
    val files = List(smallFile, largeFile)

    // Mock properties
    val props = new Properties()
    props.setProperty("directory", "/test/dir")

    // Mock behavior
    when(producerManager.createReportProducer(any[Properties])).thenReturn(standardProducer)
    when(producerManager.createBigReportProducer(any[Properties])).thenReturn(bigReportProducer)
    when(fileHandler.listXmlFiles(any[String])).thenReturn(IO.pure(files))
    when(fileHandler.getFileSize(smallFile)).thenReturn(IO.pure(1000L)) // Small file
    when(fileHandler.getFileSize(largeFile)).thenReturn(IO.pure(2000000L)) // Large file
    when(fileHandler.readFileContentAsString(smallFile)).thenReturn(IO.pure("small content"))
    when(fileHandler.readFileContentAsString(largeFile)).thenReturn(IO.pure("large content"))
    when(producerManager.makeHeaders(any[String])).thenReturn(Map.empty[String, String])

    // Create SUT with mocks
    val kafkaManager = new KafkaManager {
      override private[broker] def createProducerManager() = producerManager
      override private[broker] def createFileHandler() = fileHandler
    }

    // Execute
    kafkaManager.produceRecords(props).unsafeRunSync()

    // Verify
    verify(standardProducer).send(
      eqTo("small content"),
      any[Map[String, String]]
    )

    val streamingCaptor = ArgumentCaptor.forClass(classOf[ByteArrayInputStream])
    verify(bigReportProducer).send(
      streamingCaptor.capture(),
      any[Map[String, String]]
    )

    // The captured input stream should contain "large content"
    val bytes = new Array[Byte](streamingCaptor.getValue.available())
    streamingCaptor.getValue.read(bytes)
    new String(bytes) should include("large content")

    verify(bigReportProducer).close()
    verify(standardProducer).close()
  }
}
