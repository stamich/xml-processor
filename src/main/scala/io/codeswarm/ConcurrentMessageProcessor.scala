package io.codeswarm

import cats.effect.implicits.concurrentParTraverseOps
import cats.effect.{ExitCode, IO, IOApp}
import io.codeswarm.broker.{ConcurrentKafkaManager, KafkaManager}
import io.codeswarm.file.{FileHandler, PropertiesHandler}

import scala.concurrent.duration.DurationInt

object ConcurrentMessageProcessor extends IOApp {

  private val propsPath = "/kafka.properties"
  private val pollInterval = 5.seconds
  private val maxConcurrent = 4

  override def run(args: List[String]): IO[ExitCode] = {
    def processFiles(kafka: ConcurrentKafkaManager, fileHandler: FileHandler) = {
      for {
        files <- fileHandler.listXmlFiles("data")
        _ <- files.parTraverseN(maxConcurrent)(file =>
          fileHandler.readFileContentAsString(file)
            .flatMap(content => kafka.sendRecord(file.getName, content))
            .handleErrorWith(err =>
              IO.println(s"Failed to process ${file.getName}: ${err.getMessage}")
            )
        )
      } yield ()
    }

    val program = for {
      properties <- IO(new PropertiesHandler().loadProperties(propsPath))
        .handleErrorWith(err =>
          IO.raiseError(new RuntimeException(s"Failed to load properties: ${err.getMessage}"))
        )
      kafka = new ConcurrentKafkaManager
      fileHandler = new FileHandler
      fiber <- (processFiles(kafka, fileHandler) >>
        IO.sleep(pollInterval))
        .foreverM
        .start
      _ <- IO.println("Processing started. Press Enter to stop...") >>
        IO.readLine >>
        fiber.cancel
    } yield ExitCode.Success

    program.handleErrorWith { err =>
      IO.println(s"Application error: ${err.getMessage}")
        .as(ExitCode.Error)
    }
  }
}
