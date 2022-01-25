/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
name := "Yosegi Spark v3.0.0"
version := "2.0.0-SNAPSHOT"
scalaVersion := "2.12.14"
fork := true
organization := "jp.co.yahoo.yosegi"

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-encoding", "UTF-8")

libraryDependencies += "org.junit.jupiter" % "junit-jupiter-api" % "5.8.2" % "test"
libraryDependencies += "org.junit.jupiter" % "junit-jupiter-engine" % "5.8.2" % "test"
libraryDependencies += "org.junit.jupiter" % "junit-jupiter-params" % "5.8.2" % "test"
libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % Test exclude("junit", "junit-dep") 

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.13.2"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.2.1"
libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.13.2"

libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "3.2.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.2.1"

libraryDependencies += "jp.co.yahoo.yosegi" % "yosegi" % "2.0.2-SNAPSHOT"


// release for Maven
publishMavenStyle := true
Test / publishArtifact := false
pomIncludeRepository := { _ => false }


description := "Yosegi package."
licenses := List("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
homepage := Some(url("https://github.com/yahoojapan/yosegi-spark"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/yahoojapan/yosegi-spark"),
    "scm:git@github.com:yahoojapan/yosegi-spark.git"
  )
)

developers := List(
  Developer(
    id    = "koijima",
    name  = "Koji Ijima",
    email = "kijima@yahoo-corp.jp",
    url   = url("https://github.com/koijima")
  )
)

publishMavenStyle := true
publishArtifact in Test := false
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

