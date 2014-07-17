name := """data-exporter-service"""

version := "1.0.2"

organization := "com.blinkboxbooks.platform.services"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "com.typesafe"            %% "scalalogging-slf4j"   % "1.0.1",
  "org.slf4j"                % "slf4j-log4j12"        % "1.7.5",
  "log4j"                    % "log4j"                % "1.2.17",
  "org.scalatest"           %% "scalatest"            % "1.9.1" % "test",
  "junit"                    % "junit"                % "4.11" % "test",
  "com.novocode"             % "junit-interface"      % "0.10" % "test",
  "com.rabbitmq"             % "amqp-client"          % "3.1.4",
  "org.squeryl"             %% "squeryl"              % "0.9.5-6",
  "com.h2database"           % "h2"                   % "1.3.173" % "test",
  "mysql"                    % "mysql-connector-java" % "5.1.26",
  "commons-dbcp"             % "commons-dbcp"         % "1.4",
  "com.typesafe"             % "config"               % "1.0.2",
  "org.mockito"              % "mockito-core"         % "1.9.5" % "test",
  "com.netflix.rxjava"       % "rxjava-scala"         % "0.16.1",
  "it.sauronsoftware.cron4j" % "cron4j"               % "2.2.5"
)

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")

parallelExecution := false

net.virtualvoid.sbt.graph.Plugin.graphSettings

// Add current working directory to classpath for JAR file.
packageOptions in (Compile, packageBin) +=
    Package.ManifestAttributes( java.util.jar.Attributes.Name.CLASS_PATH -> "." )

// Leave out bits we don't care about.
publishArtifact in (Compile, packageDoc) := false

publishArtifact in (Compile, packageSrc) := false
