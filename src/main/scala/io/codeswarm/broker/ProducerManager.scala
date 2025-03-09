package io.codeswarm.broker

import io.codeswarm.api.{EmbProducer, EmbStreamingProducer}
import io.codeswarm.api.dto.EmbHeaders
import io.codeswarm.client.EmbProducers
import io.codeswarm.client.serialization.StringSerializer
import io.codeswarm.storage.DefaultStorageServiceClient

import java.util.Properties

class ProducerManager {

  def createReportProducer(props: Properties): EmbProducer[String] = {
    val client = new DefaultStorageServiceClient(props)
    val producer = EmbProducers.newProducerWithBigMessageSupport(new StringSerializer)
      .withStorageService(client)
      .withProperties(props)
      .build()
    producer
  }

  def createBigReportProducer(props: Properties): EmbStreamingProducer = {
    val client = new DefaultStorageServiceClient(props)
    val producer = EmbProducers.newStreamingProducerWithBigMessageSupport()
      .withStorageService(client)
      .withProperties(props)
      .build()
    producer
  }

  def makeHeaders(filename: String): EmbHeaders = {
    val headers = EmbHeaders.headersBuilder()
      .addHeader(filename, "")
      .build()
    headers
  }
}
