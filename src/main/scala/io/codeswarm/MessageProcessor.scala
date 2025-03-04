package io.codeswarm

import cats.effect.{ExitCode, IO, IOApp}
import io.codeswarm.broker.KafkaManager
import io.codeswarm.file.PropertiesHandler

object MessageProcessor extends IOApp {

  private val propsPath = "/kafka.properties"

  override def run(args: List[String]): IO[ExitCode] = {
    val properties = new PropertiesHandler()
    val kafka = new KafkaManager

    kafka.produceRecords(properties.loadProperties(propsPath))
      .as(ExitCode.Success)
  }
}