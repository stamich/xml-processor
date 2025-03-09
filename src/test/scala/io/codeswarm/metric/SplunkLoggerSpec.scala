package io.codeswarm.metric

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.effect.testing.scalatest.AsyncIOSpec
import io.circe.Json
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.http4s.client.Client
import org.http4s.{Request, Response, Status, Uri}
import org.mockito.{ArgumentCaptor, ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class SplunkLoggerSpec extends AsyncFlatSpec with AsyncIOSpec with MockitoSugar
  with ArgumentMatchersSugar with Matchers with BeforeAndAfterEach {

  // Mock FileSystem
  val mockFs = mock[FileSystem]

  // Helper function to create a test FileSystem
  def createTestFileSystem(): Unit = {
    val mockConf = mock[Configuration]

    // Mock the static FileSystem.get method
    val fsField = classOf[SplunkLogger.type].getDeclaredField("fileSystemProvider")
    fsField.setAccessible(true)
    fsField.set(SplunkLogger, (conf: Configuration) => mockFs)
  }

  override def beforeEach(): Unit = {
    reset(mockFs)
    createTestFileSystem()
  }

  "countXmlFiles" should "return the correct count of XML files" in {
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

  "extractTimestamps" should "extract correct timestamps for kafka connector" in {
    val output = """
      [2023-04-01T12:00:00.000] Processing files from /test/path.har
      ... some log output ...
      [2023-04-01T12:05:30.000] All files processed successfully
    """

    val (start, end) = SplunkLogger.extractTimestamps(output, "kafka")

    start shouldBe "2023-04-01T12:00:00.000"
    end shouldBe "2023-04-01T12:05:30.000"
  }

  it should "extract correct timestamps for non-kafka connector" in {
    val output = """
      [2023-04-01T12:00:00.000] Start sending test reports
      ... some log output ...
      [2023-04-01T12:05:30.000] All reports successfully sent
    """

    val (start, end) = SplunkLogger.extractTimestamps(output, "http")

    start shouldBe "2023-04-01T12:00:00.000"
    end shouldBe "2023-04-01T12:05:30.000"
  }

  "calculateDuration" should "return correct duration in seconds" in {
    val start = "2023-04-01T12:00:00.000"
    val end = "2023-04-01T12:05:30.000"

    val duration = SplunkLogger.calculateDuration(start, end)

    duration shouldBe 330 // 5 minutes and 30 seconds
  }

  "sendToSplunk" should "send the correct payload to Splunk" in {
    // Setup mock client
    val mockClient = mock[Client[IO]]
    when(mockClient.status(any[Request[IO]])).thenReturn(IO.pure(Status.Ok))

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
    verify(mockClient).status(requestCaptor.capture())

    val request = requestCaptor.getValue
    request.method.name shouldBe "POST"
    request.uri.toString shouldBe "https://splunk-api.example.com/services/collector"

    val authHeader = request.headers.get("Authorization").get.head.value
    authHeader shouldBe "Bearer test-token"
  }

  "logToSplunk" should "process data and send to Splunk" in {
    // Setup
    val fileStatuses = Array(
      createFileStatus("file1.xml"),
      createFileStatus("file2.xml")
    )

    when(mockFs.exists(any[Path])).thenReturn(true)
    when(mockFs.listStatus(any[Path])).thenReturn(fileStatuses)

    // Mock the HTTP client
    val mockClientProvider = mock[ClientProvider]
    when(mockClientProvider.getClient()).thenReturn(IO.pure(mock[Client[IO]]))
    when(mockClientProvider.getClient().flatMap(any[Client[IO] => IO[Unit]]())(any())).thenReturn(IO.unit)

    val splunkLoggerWithMocks = new {
      def testLogToSplunk(): IO[Unit] = {
        SplunkLogger.logToSplunk(
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
          clientProvider = mockClientProvider
        )
      }
    }

    // Execute
    val result = splunkLoggerWithMocks.testLogToSplunk().attempt.unsafeRunSync()

    // Verify
    result should be(Symbol("right"))
    verify(mockFs).listStatus(any[Path])
    verify(mockClientProvider).getClient()
  }

  // Helper methods
  private def createFileStatus(fileName: String): FileStatus = {
    val status = mock[FileStatus]
    val path = mock[Path]
    when(path.getName).thenReturn(fileName)
    when(status.getPath).thenReturn(path)
    status
  }

  // Mock of client provider for testing
  trait ClientProvider {
    def getClient(): IO[Client[IO]]
  }
}