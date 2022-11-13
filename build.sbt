ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"
val sparkVersion = "3.3.1"
val slf4jVersion = "2.0.3"

lazy val root = (project in file("."))
  .settings(
    name := "spark-basics"
  )

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  //Spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  //Logging
  "org.slf4j" % "slf4j-simple" % slf4jVersion,
  "org.slf4j" % "slf4j-api" % slf4jVersion,
)