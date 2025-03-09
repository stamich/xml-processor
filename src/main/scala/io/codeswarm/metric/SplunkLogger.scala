package io.codeswarm.metric

import cats.effect.IO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.http4s.client.Client
import org.http4s.{Method, Request, Uri}
import org.http4s.headers.Authorization
import org.http4s.AuthScheme

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.nio.charset.StandardCharsets

object SplunkLogger {
  // For testing
  private var fileSystemProvider: Configuration => FileSystem = (conf: Configuration) => FileSystem.get(conf)

  def countXmlFiles(harPath: String): IO[Int] = IO {
    val conf = new Configuration()
    val fs = fileSystemProvider(conf)
    val path = new Path(harPath)

    if (fs.exists(path)) {
      val fileStatuses = fs.listStatus(path)
      fileStatuses.count(_.getPath.getName.endsWith(".xml"))
    } else 0
  }

  def extractTimestamps(output: String, connectorType: String): (String, String) = {
    val pattern = "\\[(.*?)\\]".r

    val timestamps = pattern.findAllMatchIn(output)
      .map(_.group(1))
      .toList

    val first = timestamps.headOption.getOrElse("")
    val last = timestamps.lastOption.getOrElse("")

    (first, last)
  }

  def calculateDuration(start: String, end: String): Long = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
    val startTime = LocalDateTime.parse(start, formatter)
    val endTime = LocalDateTime.parse(end, formatter)
    ChronoUnit.SECONDS.between(startTime, endTime)
  }

  def sendToSplunk(
                    client: Client[IO],
                    event: SplunkEvent,
                    splunkUrl: String,
                    splunkToken: String,
                    splunkIndex: String
                  ): IO[String] = {
    // Convert event to JSON string manually without circe
    val jsonEvent = s"""
                       |{
                       |  "event": {
                       |    "workflow_id": "${event.workflow_id}",
                       |    "business_date": "${event.business_date}",
                       |    "ingestion_time": "${event.ingestion_time}",
                       |    "source_system": "${event.source_system}",
                       |    "connector_type": "${event.connector_type}",
                       |    "reports_count": ${event.reports_count},
                       |    "start_time": "${event.start_time}",
                       |    "end_time": "${event.end_time}",
                       |    "duration_seconds": ${event.duration_seconds},
                       |    "timestamp": "${event.timestamp}"
                       |  },
                       |  "sourcetype": "emb_metrics",
                       |  "index": "$splunkIndex"
                       |}""".stripMargin

    val request = Request[IO](Method.POST, Uri.unsafeFromString(splunkUrl))
      .withHeaders(Authorization(org.http4s.Credentials.Token(AuthScheme.Bearer, splunkToken)))
      .withEntity(jsonEvent)

    client.expect[String](request)
  }

  // Define the ClientProvider trait
  trait ClientProvider {
    def getClient(): IO[Client[IO]]
  }

  // Main method for sending metrics to Splunk
  def logToSplunk(
                   workflowId: String,
                   businessDate: String,
                   ingestionTime: String,
                   sourceSystem: String,
                   connectorType: String,
                   executionOutput: String,
                   harPath: String,
                   splunkUrl: String,
                   splunkToken: String,
                   splunkIndex: String,
                   clientProvider: ClientProvider
                 ): IO[Unit] = {
    for {
      count <- countXmlFiles(harPath)
      (startTime, endTime) = extractTimestamps(executionOutput, connectorType)
      durationSeconds = calculateDuration(startTime, endTime)
      timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"))

      event = SplunkEvent(
        workflow_id = workflowId,
        business_date = businessDate,
        ingestion_time = ingestionTime,
        source_system = sourceSystem,
        connector_type = connectorType,
        reports_count = count,
        start_time = startTime,
        end_time = endTime,
        duration_seconds = durationSeconds,
        timestamp = timestamp
      )

      client <- clientProvider.getClient()
      _ <- sendToSplunk(client, event, splunkUrl, splunkToken, splunkIndex)
    } yield ()
  }
}