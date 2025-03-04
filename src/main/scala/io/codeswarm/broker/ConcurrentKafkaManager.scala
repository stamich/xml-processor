package io.codeswarm.broker

import cats.effect.IO
import cats.implicits.toTraverseOps
import io.codeswarm.file.FileHandler
import org.apache.kafka.clients.producer.ProducerRecord

import java.util.Properties

class ConcurrentKafkaManager {

  private val producerManager = new ProducerManager
  private lazy val producer = producerManager.createProducer()

  def sendRecord(key: String, value: String): IO[Unit] = IO {
    producer.send(new ProducerRecord[String, String]("topic", key, value)).get()
  }

  def produceRecords(props: Properties): IO[Unit] = {
    val fileHandler = new FileHandler
    for {
      files <- fileHandler.listXmlFiles("data")
      _ <- files.traverse(file =>
        fileHandler.readFileContentAsString(file)
          .flatMap(content => sendRecord(file.getName, content))
      )
    } yield ()
  }
}
