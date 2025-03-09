package io.codeswarm.metric

import cats.effect.{IO, Resource}
import io.circe.syntax._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.http4s.Method._
import org.http4s._
import org.http4s.circe._
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.headers._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.concurrent.ExecutionContext.global

object SplunkLogger {

  private val dateTimePattern = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")

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
    splunkIndex: String
    ): IO[Unit] = {
    val client = BlazeClientBuilder[IO](global).resource

    for {
      reportsCount <- countXmlFiles(harPath)
      (startTime, endTime) = extractTimestamps(executionOutput, connectorType)
      duration = calculateDuration(startTime, endTime)
      now = LocalDateTime.now().format(dateTimePattern)

      event = SplunkEvent(
        workflow_id = workflowId,
        business_date = businessDate,
        ingestion_time = ingestionTime,
        source_system = sourceSystem,
        connector_type = connectorType,
        reports_count = reportsCount,
        start_time = startTime,
        end_time = endTime,
        duration_seconds = duration,
        timestamp = now
      )

      _ <- sendToSplunk(client, event, splunkUrl, splunkToken, splunkIndex)
    } yield ()
  }

  private def countXmlFiles(harPath: String): IO[Int] = IO {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val path = new Path(harPath)

    if (fs.exists(path)) {
      val fileStatuses = fs.listStatus(path)
      fileStatuses.count(_.getPath.getName.endsWith(".xml"))
    } else 0
  }

  private def extractTimestamps(output: String, connectorType: String): (String, String) = {
    val startPattern = if (connectorType == "kafka")
      "\\[(.*?)\\].*Processing files from".r
    else
      "\\[(.*?)\\].*Start sending .* reports".r

    val endPattern = if (connectorType == "kafka")
      "\\[(.*?)\\].*All files processed successfully".r
    else
      "\\[(.*?)\\].*All reports successfully sent".r

    val startMatch = startPattern.findFirstMatchIn(output).map(_.group(1))
    val endMatch = endPattern.findFirstMatchIn(output).map(_.group(1))

    val now = LocalDateTime.now().format(dateTimePattern)
    (startMatch.getOrElse(now), endMatch.getOrElse(now))
  }

  private def calculateDuration(start: String, end: String): Long = {
    try {
      val startDt = LocalDateTime.parse(start, dateTimePattern)
      val endDt = LocalDateTime.parse(end, dateTimePattern)
      java.time.Duration.between(startDt, endDt).getSeconds
    } catch {
      case _: Exception => 0L
    }
  }

  private def sendToSplunk(
                            clientR: Resource[IO, Client[IO]],
                            event: SplunkEvent,
                            splunkUrl: String,
                            token: String,
                            index: String
                          ): IO[Unit] = {
    val payload = Map(
      "event" -> event.asJson,
      "sourcetype" -> "ebridge_connector_logs".asJson,
      "index" -> index.asJson,
      "source" -> "oozie_workflow".asJson
    ).asJson

    val request = Request[IO](POST, Uri.unsafeFromString(splunkUrl))
      .withHeaders(
        Authorization(Credentials.Token(AuthScheme.Bearer, token)),
        `Content-Type`(MediaType.application.json)
      )
      .withEntity(payload)

    clientR.use { client =>
      client.status(request).flatMap {
        case Status.Ok => IO.unit
        case status => IO.raiseError(new Exception(s"Failed to send to Splunk: $status"))
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val usage = """
                  |Usage: SplunkLogger --workflow-id=<id> --business-date=<date> --ingestion-time=<time>
                  |                     --source-system=<sys> --connector-type=<type> --execution-output=<output>
                  |                     --har-path=<path> --splunk-url=<url> --splunk-token=<token> --splunk-index=<index>
      """.stripMargin

    if (args.length < 10) {
      println(usage)
      sys.exit(1)
    }

    val argMap = args.map { arg =>
      val parts = arg.split("=", 2)
      if (parts.length == 2) parts(0).drop(2) -> parts(1)
      else parts(0).drop(2) -> ""
    }.toMap

    logToSplunk(
      workflowId = argMap.getOrElse("workflow-id", "unknown"),
      businessDate = argMap.getOrElse("business-date", "unknown"),
      ingestionTime = argMap.getOrElse("ingestion-time", "unknown"),
      sourceSystem = argMap.getOrElse("source-system", "unknown"),
      connectorType = argMap.getOrElse("connector-type", "unknown"),
      executionOutput = argMap.getOrElse("execution-output", ""),
      harPath = argMap.getOrElse("har-path", ""),
      splunkUrl = argMap.getOrElse("splunk-url", ""),
      splunkToken = argMap.getOrElse("splunk-token", ""),
      splunkIndex = argMap.getOrElse("splunk-index", "main")
    ).unsafeRunSync()
  }
}
