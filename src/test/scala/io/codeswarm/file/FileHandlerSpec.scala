package io.codeswarm.file

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentMatchers, MockitoSugar}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, File}
import java.net.URI
import java.nio.file.{Files, Paths}

class FileHandlerSpec extends AnyFunSuite with MockitoSugar with Matchers with BeforeAndAfterAll {

  // Setup test directory and files
  private val testDir = new File("test-dir")
  private val xmlFile1 = new File(testDir, "test1.xml")
  private val xmlFile2 = new File(testDir, "test2.xml")
  private val nonXmlFile = new File(testDir, "test.txt")
  private val testContent = "<root><node>test content</node></root>"

  override def beforeAll(): Unit = {
    // Create test directory and files
    testDir.mkdir()
    Files.write(xmlFile1.toPath, testContent.getBytes)
    Files.write(xmlFile2.toPath, testContent.getBytes)
    Files.write(nonXmlFile.toPath, "non-xml content".getBytes)
  }

  override def afterAll(): Unit = {
    // Clean up test files
    xmlFile1.delete()
    xmlFile2.delete()
    nonXmlFile.delete()
    testDir.delete()
  }

  test("listXmlFiles should return only XML files") {
    val fileHandler = new FileHandler()

    val result = fileHandler.listXmlFiles(testDir.getPath).unsafeRunSync()

    result.size shouldBe 2
    result.map(_.getName).toSet shouldBe Set("test1.xml", "test2.xml")
  }

  test("readFileContent should read file as bytes") {
    val fileHandler = new FileHandler()

    val result = fileHandler.readFileContent(xmlFile1).unsafeRunSync()

    result shouldBe testContent.getBytes
  }

  test("readFileContentAsString should read file as string") {
    val fileHandler = new FileHandler()

    val result = fileHandler.readFileContentAsString(xmlFile1).unsafeRunSync()

    result shouldBe testContent
  }

  test("getFileSize should return correct file size") {
    val fileHandler = new FileHandler()

    val result = fileHandler.getFileSize(xmlFile1).unsafeRunSync()

    result shouldBe testContent.getBytes.length
  }

  test("listXmlFilesFromHar should return XML files from HAR") {
    // Setup mocks
    val fs = mock[FileSystem]
    val fileHandler = spy(new FileHandler)
    val harPath = "har:///path/to/archive.har"

    // Create mock file statuses
    val xmlPath1 = new Path("file1.xml")
    val xmlPath2 = new Path("file2.xml")
    val txtPath = new Path("file.txt")

    val xmlStatus1 = mock[FileStatus]
    val xmlStatus2 = mock[FileStatus]
    val txtStatus = mock[FileStatus]

    when(xmlStatus1.getPath).thenReturn(xmlPath1)
    when(xmlStatus2.getPath).thenReturn(xmlPath2)
    when(txtStatus.getPath).thenReturn(txtPath)

    val fileStatuses = Array(xmlStatus1, xmlStatus2, txtStatus)

    // Mock static FileSystem.get
    doReturn(fs).when(fileHandler).getFileSystem(ArgumentMatchers.any[String])
    when(fs.listStatus(any[Path])).thenReturn(fileStatuses)

    // Execute
    val result = fileHandler.listXmlFilesFromHar(harPath).unsafeRunSync()

    // Verify
    result.size shouldBe 2
    result.map(_.getName).toSet shouldBe Set("file1.xml", "file2.xml")
  }

  test("readFileContentFromHar should read content from HAR file") {
    // Setup mocks
    val fs = mock[FileSystem]
    val fileHandler = spy(new FileHandler)
    val harPath = "har:///path/to/archive.har"
    val file = new File("test.xml")
    val expectedContent = "<xml>test content</xml>"
    val contentStream = new ByteArrayInputStream(expectedContent.getBytes)

    // Mock behavior
    doReturn(fs).when(fileHandler).getFileSystem(ArgumentMatchers.any[String])
    when(fs.open(any[Path])).thenReturn(new org.apache.hadoop.fs.FSDataInputStream(contentStream))

    // Execute
    val result = fileHandler.readFileContentFromHar(harPath, file).unsafeRunSync()

    // Verify
    result shouldBe expectedContent
  }

  // Helper method to make FileHandler more testable
  class TestableFileHandler extends FileHandler {
    override def getFileSystem(harPath: String): FileSystem = {
      val conf = new Configuration()
      FileSystem.get(URI.create(harPath), conf)
    }

    override def listXmlFilesFromHar(harPath: String): IO[List[File]] = IO {
      val fs = getFileSystem(harPath)
      val harFilePath = new Path(harPath)

      val fileStatuses = fs.listStatus(harFilePath)
      fileStatuses.filter(_.getPath.getName.endsWith(".xml"))
        .map(status => new File(status.getPath.getName))
        .toList
    }

    override def readFileContentFromHar(harPath: String, file: File): IO[String] = IO {
      val fs = getFileSystem(harPath)
      val filePath = new Path(s"$harPath/${file.getName}")

      val inputStream = fs.open(filePath)
      val content = scala.io.Source.fromInputStream(inputStream).mkString
      inputStream.close()
      content
    }
  }
}