// ConcurrentKafkaManagerWithRemoval.scala
package io.codeswarm.broker

import cats.effect.IO
import cats.effect.implicits._
import cats.implicits._
import io.codeswarm.file.FileHandler
import io.codeswarm.parser.XmlParser
import org.json.JSONObject
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import java.util.Properties
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.{OutputKeys, TransformerFactory}
import javax.xml.transform.stream.StreamResult

class ConcurrentKafkaManagerWithRemoval {
  private val topic = "test"
  private val maxFileSize = 1 * 1024 * 1024
  private val xmlParser = new XmlParser()

  private def removeWhitespacesFromXml(xmlContent: String): IO[String] = IO {
    val factory = DocumentBuilderFactory.newInstance()
    factory.setIgnoringElementContentWhitespace(true)
    val builder = factory.newDocumentBuilder()
    val document = builder.parse(new ByteArrayInputStream(xmlContent.getBytes))

    document.getDocumentElement.normalize()
    xmlParser.removeWhitespaceNodes(document.getDocumentElement)

    val transformerFactory = TransformerFactory.newInstance()
    val transformer = transformerFactory.newTransformer()
    transformer.setOutputProperty(OutputKeys.INDENT, "no")

    val source = new DOMSource(document)
    val output = new ByteArrayOutputStream()
    val result = new StreamResult(output)
    transformer.transform(source, result)

    output.toString
  }

  private def sendRecord(file: String, content: String, harPath: String, producers: Producers): IO[Unit] = {
    for {
      size <- IO(content.getBytes.length)
      cleanedXml <- removeWhitespacesFromXml(content)
      _ <- if (size > maxFileSize) {
        for {
          inputStream <- IO(new ByteArrayInputStream(cleanedXml.getBytes))
          _ <- IO(producers.bigReportProducer.send(inputStream, producers.producerManager.makeHeaders(file)))
          _ <- IO.println(s"Sent large file $file to Kafka")
        } yield ()
      } else {
        for {
          _ <- IO(producers.standardProducer.send(cleanedXml, producers.producerManager.makeHeaders(file)))
          _ <- IO.println(s"Sent regular file $file to Kafka")
        } yield ()
      }
    } yield ()
  }

  private case class Producers(
                                producerManager: ProducerManager,
                                standardProducer: io.codeswarm.api.EmbProducer[String],
                                bigReportProducer: io.codeswarm.api.EmbStreamingProducer
                              )

  def produceRecords(props: Properties, harPath: String, maxConcurrent: Int): IO[Unit] = {
    val producerManager = new ProducerManager()
    val standardProducer = producerManager.createReportProducer(props)
    val bigReportProducer = producerManager.createBigReportProducer(props)
    val producers = Producers(producerManager, standardProducer, bigReportProducer)

    val fileHandler = new FileHandler()

    val processFiles = for {
      files <- fileHandler.listXmlFilesFromHar(harPath)
      startTime = System.currentTimeMillis()
      _ <- IO.println(s"[${java.time.LocalDateTime.now()}]: Processing files from $harPath")
      _ <- files.parTraverseN(maxConcurrent) { file =>
        for {
          content <- fileHandler.readFileContentFromHar(harPath, file)
          _ <- sendRecord(file.getName, content, harPath, producers)
        } yield ()
      }
      endTime = System.currentTimeMillis()
      _ <- IO.println(s"[${java.time.LocalDateTime.now()}]: All files processed successfully")
      _ <- IO.println(s"Processed ${files.size} files in ${(endTime - startTime) / 1000.0} seconds")

      // Create metrics for Splunk
      metrics = new JSONObject()
        .put("reports_count", files.size)
        .put("start_timestamp", startTime)
        .put("end_timestamp", endTime)
        .put("duration_seconds", (endTime - startTime) / 1000.0)

      _ <- IO.println(s"Metrics: $metrics")
    } yield ()

    processFiles.guaranteeCase(_ => IO {
      bigReportProducer.close()
      standardProducer.close()
    })
  }

  private[broker] def createProducerManager(): ProducerManager = new ProducerManager()
  private[broker] def createFileHandler(): FileHandler = new FileHandler()
  private[broker] def createXmlParser(): XmlParser = new XmlParser()
}