package io.codeswarm.metric

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.http4s.client.Client
import org.http4s.{Request, Response, Status}
import org.mockito.{ArgumentCaptor, ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.nio.charset.StandardCharsets

class SplunkLoggerSpec extends AnyFunSuite with MockitoSugar
  with ArgumentMatchersSugar with Matchers with BeforeAndAfterEach {

  // Mock FileSystem
  val mockFs = mock[FileSystem]

  // Helper function to create a test FileSystem
  def createTestFileSystem(): Unit = {
    val fsField = classOf[SplunkLogger.type].getDeclaredField("fileSystemProvider")
    fsField.setAccessible(true)
    fsField.set(SplunkLogger, (_: Configuration) => mockFs)
  }

  override def beforeEach(): Unit = {
    reset(mockFs)
    createTestFileSystem()
  }

  test("countXmlFiles should return the correct count of XML files") {
    // Setup
    val harPath = "/test/path.har"
    val fileStatuses = Array(
      createFileStatus("file1.xml"),
      createFileStatus("file2.xml"),
      createFileStatus("file3.txt")
    )

    when(mockFs.exists(any[Path])).thenReturn(true)
    when(mockFs.listStatus(any[Path])).thenReturn(fileStatuses)

    // Execute
    val result = SplunkLogger.countXmlFiles(harPath).unsafeRunSync()

    // Verify
    result shouldBe 2
    verify(mockFs).listStatus(eqTo(new Path(harPath)))
  }

  test("extractTimestamps should extract correct timestamps for kafka connector") {
    val output = """
      [2023-04-01T12:00:00.000] Processing files from /test/path.har
      ... some log output ...
      [2023-04-01T12:05:30.000] All files processed successfully
    """

    val (start, end) = SplunkLogger.extractTimestamps(output, "kafka")

    start shouldBe "2023-04-01T12:00:00.000"
    end shouldBe "2023-04-01T12:05:30.000"
  }

  test("extractTimestamps should extract correct timestamps for non-kafka connector") {
    val output = """
      [2023-04-01T12:00:00.000] Start sending test reports
      ... some log output ...
      [2023-04-01T12:05:30.000] All reports successfully sent
    """

    val (start, end) = SplunkLogger.extractTimestamps(output, "http")

    start shouldBe "2023-04-01T12:00:00.000"
    end shouldBe "2023-04-01T12:05:30.000"
  }

  test("calculateDuration should return correct duration in seconds") {
    val start = "2023-04-01T12:00:00.000"
    val end = "2023-04-01T12:05:30.000"

    val duration = SplunkLogger.calculateDuration(start, end)

    duration shouldBe 330 // 5 minutes and 30 seconds
  }

  test("sendToSplunk should send the correct payload to Splunk") {
    // Setup mock client
    val mockClient = mock[Client[IO]]
    when(mockClient.expect[String](any[Request[IO]])).thenReturn(IO.pure("Success"))

    val event = SplunkEvent(
      workflow_id = "test-workflow",
      business_date = "2023-04-01",
      ingestion_time = "2023-04-01T12:00:00.000",
      source_system = "test-system",
      connector_type = "kafka",
      reports_count = 10,
      start_time = "2023-04-01T12:00:00.000",
      end_time = "2023-04-01T12:05:30.000",
      duration_seconds = 330,
      timestamp = "2023-04-01T12:05:35.000"
    )

    // Execute
    SplunkLogger.sendToSplunk(
      mockClient,
      event,
      "https://splunk-api.example.com/services/collector",
      "test-token",
      "test-index"
    ).unsafeRunSync()

    // Verify
    val requestCaptor = ArgumentCaptor.forClass(classOf[Request[IO]])
    verify(mockClient).expect[String](requestCaptor.capture())

    val request = requestCaptor.getValue
    request.method.name shouldBe "POST"
    request.uri.toString shouldBe "https://splunk-api.example.com/services/collector"

    val authHeader = request.headers.get("Authorization").get.head.value
    authHeader shouldBe "Bearer test-token"
  }

  test("logToSplunk should process data and send to Splunk") {
    // Setup
    val fileStatuses = Array(
      createFileStatus("file1.xml"),
      createFileStatus("file2.xml")
    )

    when(mockFs.exists(any[Path])).thenReturn(true)
    when(mockFs.listStatus(any[Path])).thenReturn(fileStatuses)

    // Mock the HTTP client
    val mockHttpClient = mock[Client[IO]]
    when(mockHttpClient.expect[String](any[Request[IO]])).thenReturn(IO.pure("Success"))

    val clientProvider = new TestClientProvider(mockHttpClient)

    // Execute
    val result = SplunkLogger.logToSplunk(
      workflowId = "test-workflow",
      businessDate = "2023-04-01",
      ingestionTime = "2023-04-01T12:00:00.000",
      sourceSystem = "test-system",
      connectorType = "kafka",
      executionOutput = """
        [2023-04-01T12:00:00.000] Processing files from /test/path.har
        [2023-04-01T12:05:30.000] All files processed successfully
      """,
      harPath = "/test/path.har",
      splunkUrl = "https://splunk-api.example.com/services/collector",
      splunkToken = "test-token",
      splunkIndex = "test-index",
      clientProvider = clientProvider
    ).attempt.unsafeRunSync()

    // Verify
    result should be(Symbol("right"))
    verify(mockFs).listStatus(any[Path])
    verify(mockHttpClient).expect[String](any[Request[IO]])
  }

  // Helper methods
  private def createFileStatus(fileName: String): FileStatus = {
    val status = mock[FileStatus]
    val path = mock[Path]
    when(path.getName).thenReturn(fileName)
    when(status.getPath).thenReturn(path)
    status
  }

  // Test implementation of client provider
  class TestClientProvider(mockClient: Client[IO]) extends SplunkLogger.ClientProvider {
    override def getClient(): IO[Client[IO]] = IO.pure(mockClient)
  }
}