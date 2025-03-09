package io.codeswarm

import cats.effect.{ExitCode, IO, IOApp}
import io.codeswarm.broker.ConcurrentKafkaManager
import io.codeswarm.file.{FileHandler, PropertiesHandler}
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

object ConcurrentMessageProcessor extends IOApp {
  private val propsPath = "/kafka.properties"
  private val maxConcurrent = 4
  private val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")

  override def run(args: List[String]): IO[ExitCode] = {
    val harPath = if (args.length > 0) args(0) else ""

    def logTimestamp(message: String): IO[Unit] = {
      val timestamp = LocalDateTime.now().format(dateTimeFormatter)
      IO.println(s"[$timestamp]: $message")
    }

    val program = for {
      _ <- logTimestamp(s"Starting ConcurrentMessageProcessor with HAR path: $harPath")
      properties <- IO(new PropertiesHandler().loadProperties(propsPath))
        .handleErrorWith(err =>
          IO.raiseError(new RuntimeException(s"Failed to load properties: ${err.getMessage}"))
        )
      kafka = new ConcurrentKafkaManager
      _ <- kafka.produceRecords(properties, harPath, maxConcurrent)
      _ <- logTimestamp("Kafka processing completed successfully")
    } yield ExitCode.Success

    program.handleErrorWith { err =>
      logTimestamp(s"Application error: ${err.getMessage}")
        .as(ExitCode.Error)
    }
  }
}
