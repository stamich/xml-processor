// ConcurrentKafkaManager.scala
package io.codeswarm.broker

import cats.effect.IO
import cats.effect.implicits.concurrentParTraverseOps
import cats.implicits._
import io.codeswarm.file.FileHandler
import org.json.JSONObject

import java.io.{ByteArrayInputStream, File}
import java.util.Properties

class ConcurrentKafkaManager {
  private val topic = "test"
  private val maxFileSize = 1 * 1024 * 1024

  private def sendRecord(file: String, content: String, harPath: String, producers: Producers): IO[Unit] = {
    for {
      size <- IO(content.getBytes.length)
      _ <- if (size > maxFileSize) {
        for {
          inputStream <- IO(new ByteArrayInputStream(content.getBytes))
          _ <- IO(producers.bigReportProducer.send(inputStream, producers.producerManager.makeHeaders(file)))
          _ <- IO.println(s"Sent large file $file to Kafka")
        } yield ()
      } else {
        for {
          _ <- IO(producers.standardProducer.send(content, producers.producerManager.makeHeaders(file)))
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

    processFiles.guarantee(IO {
      bigReportProducer.close()
      standardProducer.close()
    })
  }
}