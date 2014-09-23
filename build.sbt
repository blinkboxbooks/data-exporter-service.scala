import AssemblyKeys._

name := "data-exporter-service"

version := scala.util.Try(scala.io.Source.fromFile("VERSION").mkString.trim).getOrElse("0.0.0")

organization := "com.blinkbox.books.mimir"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "com.blinkbox.books"      %% "common-config"        % "1.2.1",
  "com.blinkbox.books"      %% "common-scala-test"    % "0.3.0" % "test",
  "org.squeryl"             %% "squeryl"              % "0.9.5-6",
  "com.h2database"           % "h2"                   % "1.3.173" % "test",
  "mysql"                    % "mysql-connector-java" % "5.1.26",
  "commons-dbcp"             % "commons-dbcp"         % "1.4",
  "com.netflix.rxjava"       % "rxjava-scala"         % "0.16.1",
  "it.sauronsoftware.cron4j" % "cron4j"               % "2.2.5"
)

rpmPrepSettings

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8", "-target:jvm-1.7")

parallelExecution := false
