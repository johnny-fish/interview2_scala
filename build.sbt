name := "XXXHomeWork"

version := "0.1"

scalaVersion := "2.13.1"
test in assembly := {}


libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % Test
libraryDependencies +=  "org.mockito" % "mockito-core" % "3.3.3" % Test
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.30"
libraryDependencies += "org.rogach" %% "scallop" % "3.4.0"
//libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.751"
mainClass in assembly := Some("MainApp")
scalacOptions += "-deprecation"


javaOptions ++= Seq("-Xms512", "-Xmx8g")