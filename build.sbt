import Dependencies._
import sbtassembly.MergeStrategy


name := """Lambda-Arch-Spark"""


spName := "knoldus/Lambda-Arch-Spark"

sparkVersion := "2.0.0"

sparkComponents ++= Seq("core","streaming", "sql")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

spIncludeMaven := true
/*
credentials += Credentials("Spark Packages Realm",
  "spark-packages.org",
  sys.props.getOrElse("GITHUB_USERNAME", default = ""),
  sys.props.getOrElse("GITHUB_PERSONAL_ACCESS_TOKEN", default = ""))
*/


lazy val commonSettings = Seq(
  organization := "com.knoldus",
  version := "0.1.0",
  scalaVersion := "2.11.8"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "Lambda-Arch-Spark",
    libraryDependencies ++= Seq(kafka,  cassandraDriver,  akkaHttpJson,  json4s, lift,kafkaClient )
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) => MergeStrategy.last
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}