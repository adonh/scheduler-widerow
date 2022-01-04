organization := "com.pagerduty"

name := "widerow"

scalaVersion := "2.11.12"

crossScalaVersions := Seq("2.10.4", "2.11.12")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % Test,
  "org.scalamock" %% "scalamock-scalatest-support" % "3.2" % Test,
  "org.scalacheck" %% "scalacheck" % "1.12.2" % Test)
