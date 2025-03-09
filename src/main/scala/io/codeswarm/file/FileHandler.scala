package io.codeswarm.file

import cats.effect.IO
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}

class FileHandler {

  def listXmlFiles(directory: String): IO[List[File]] = IO {
    new File(directory)
      .listFiles
      .filter(_.getName.endsWith(".xml"))
      .toList
  }

  def readFileContent(file:File): IO[Array[Byte]] = IO {
    Files.readAllBytes(Paths.get(file.getAbsolutePath))
  }

  def readFileContentAsString(file: File): IO[String] = IO {
    new String(Files.readAllBytes(Paths.get(file.getAbsolutePath)))
  }

  def listXmlFilesFromHar(harPath: String): IO[List[File]] = IO {
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(harPath), conf)
    val harFilePath = new Path(harPath)

    val fileStatuses = fs.listStatus(harFilePath)
    fileStatuses.filter(_.getPath.getName.endsWith(".xml"))
      .map(status => new File(status.getPath.getName))
      .toList
  }

  def readFileContentFromHar(harPath: String, file: File): IO[String] = IO {
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(harPath), conf)
    val filePath = new Path(s"$harPath/${file.getName}")

    val inputStream = fs.open(filePath)
    val content = scala.io.Source.fromInputStream(inputStream).mkString
    inputStream.close()
    content
  }

  def getFileSize(file: File): IO[Long] = IO {
    file.length()
  }
}
