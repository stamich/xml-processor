package io.codeswarm.metric

import cats.effect.IO
import org.http4s.client.Client

trait ClientProvider {
  def getClient(): IO[Client[IO]]
}
