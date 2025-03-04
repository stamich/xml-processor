package io.codeswarm.file

import org.apache.kafka.clients.producer.ProducerConfig

import java.util.Properties

class PropertiesHandler {

  def loadProperties(path: String): Properties = {
    val props = new java.util.Properties()
    props.load(getClass.getResourceAsStream(path))
    props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    props
  }
}
