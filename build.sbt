ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "hadooptask"
  )


scalaVersion := "2.12.18" // Spark 3.2.x supports Scala 2.12.x

// Define Spark version that has better compatibility with newer Java versions
val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "mysql" % "mysql-connector-java" % "8.0.33",
  "org.apache.hadoop" % "hadoop-common" % "3.3.4",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.520",
  "com.typesafe" % "config" % "1.4.2"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => xs match {
    case "MANIFEST.MF" :: Nil => MergeStrategy.discard // Custom strategy as an example
    case "module-info.class" :: Nil => MergeStrategy.concat
    case _ => MergeStrategy.discard // Or use other strategies as necessary
  }
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}