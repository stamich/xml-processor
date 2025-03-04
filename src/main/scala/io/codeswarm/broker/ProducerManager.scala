package io.codeswarm.broker

import org.apache.kafka.clients.producer.KafkaProducer

import java.util.Properties

class ProducerManager {

  def createProducer(props: Properties): KafkaProducer[String, String] = {
    new KafkaProducer[String, String](props)
  }

  def closeProducer(producer: KafkaProducer[String, String]): Unit = {
    producer.close()
  }
}
