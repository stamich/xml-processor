ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "scala-xml-processor",
    //idePackagePrefix := Some("io.codeswarm")
  )

// https://mvnrepository.com/artifact/org.scala-lang.modules/scala-xml
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "2.3.0"

// https://mvnrepository.com/artifact/org.typelevel/cats-effect
libraryDependencies += "org.typelevel" %% "cats-effect" % "3.5.7"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.9.0"

// https://mvnrepository.com/artifact/commons-codec/commons-codec
libraryDependencies += "commons-codec" % "commons-codec" % "1.18.0"

libraryDependencies += "org.json" % "json" % "20250107"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.4.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.4.1"

// HTTP client for Splunk API
libraryDependencies += "org.http4s" %% "http4s-dsl" % "0.23.30"
libraryDependencies += "org.http4s" %% "http4s-blaze-client" % "0.23.17"
libraryDependencies += "org.http4s" %% "http4s-circe" % "0.23.30"

// https://mvnrepository.com/artifact/io.grpc/grpc-stub
libraryDependencies += "io.grpc" % "grpc-stub" % "1.71.0"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test
libraryDependencies += "org.mockito" %% "mockito-scala" % "1.17.37" % Test
libraryDependencies += "org.typelevel" %% "cats-effect-testing-scalatest" % "1.6.0" % Test

