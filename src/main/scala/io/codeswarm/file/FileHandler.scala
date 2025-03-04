package io.codeswarm.file

import cats.effect.IO

import java.io.File
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

  def getFileSize(file: File): IO[Long] = IO {
    file.length()
  }
}
