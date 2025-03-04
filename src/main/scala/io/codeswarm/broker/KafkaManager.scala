package io.codeswarm.broker

import cats.effect.IO
import cats.implicits.toTraverseOps
import io.codeswarm.file.FileHandler
import org.apache.kafka.clients.producer.ProducerRecord

import java.io.{ByteArrayInputStream, File}
import java.util.Properties

class KafkaManager {

  private val topic = "test"
  private val maxFileSize = 1 * 1024 * 1024

  def produceRecords(props: Properties): IO[Unit] = IO {

    val producer = new ProducerManager().createProducer(props)
    val fileHandler = new FileHandler()
    val directory = props.getProperty("directory")

    (for {
      files <- fileHandler.listXmlFiles(directory)
      _ <- files.traverse { file =>
        for {
          size <- fileHandler.getFileSize(file)
          _ <- if (size > maxFileSize) {
            for {
              data <- fileHandler.readFileContentAsString(file)
              record = createRecord(file, data)
              //inputStream = new ByteArrayInputStream(record.value().getBytes)
              _ <- IO(producer.send(createRecord(file, data)))
            } yield ()
          } else {
            for {
              content <- fileHandler.readFileContentAsString(file)
              _ <- IO(producer.send(createRecord(file, content)))
            } yield ()
          }
        } yield ()
      }
    } yield ()).guarantee(IO { producer.close()})
  }

  private def createRecord(file: File, content: String): ProducerRecord[String, String] = {
    new ProducerRecord[String, String](topic, file.getName, content)
  }
}
