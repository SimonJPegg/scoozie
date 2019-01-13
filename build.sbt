name := "scoozie"

organization := "org.antipathy"

description := "Oozie artifact builder and validator"

scalaVersion := "2.12.8"

crossScalaVersions := Seq("2.10.7","2.11.12", scalaVersion.value)

scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-unchecked")

licenses := Seq((
  "Apache License, Version 2.0", 
  url("https://github.com/SimonJPegg/scoozie/blob/master/LICENSE.md")
))

developers := List(
  Developer(
    "SimonJPegg", 
    "Ciaran Kearney",
    "ciaran@antipathy.org",
    url("http://www.antipathy.org")
  ))

homepage := Some(url("https://github.com/SimonJPegg/scoozie"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/SimonJPegg/scoozie"), 
    "git@github.com:SimonJPegg/scoozie.git",
    Some("git@github.com:SimonJPegg/scoozie.git")
  ))

libraryDependencies := Seq(
  "org.apache.commons" % "commons-lang3" % "3.8" % "compile",
  "commons-io" % "commons-io" % "2.6" % "compile",
  "xerces" % "xercesImpl" % "2.11.0" % "compile",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

libraryDependencies := {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, scalaMajor)) if scalaMajor >= 11 =>
      libraryDependencies.value ++ Seq(
        "org.scala-lang.modules" %% "scala-xml" % "1.1.1",
        "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.1",
        "org.scala-lang.modules" %% "scala-swing" % "2.0.3")
    case _ =>
      libraryDependencies.value :+ "org.scala-lang" % "scala-swing" % "2.10.7"
  }
}

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credential")

publishTo := sonatypePublishTo.value

sonatypeProfileName := "org.antipathy"

publishMavenStyle := true

publishArtifact in Test := false

import ReleaseTransformations._

releaseCrossBuild := true 

releasePublishArtifactsAction := PgpKeys.publishSigned.value

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommandAndRemaining("+publishSigned"),
  setNextVersion,
  commitNextVersion,
  releaseStepCommand("sonatypeReleaseAll"),
  pushChanges
)

releaseCrossBuild := true

