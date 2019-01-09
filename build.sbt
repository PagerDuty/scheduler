lazy val bintraySettings = Seq(
  bintrayOrganization := Some("pagerduty"),
  bintrayRepository := "oss-maven",
  licenses += ("BSD New", url("https://opensource.org/licenses/BSD-3-Clause")),
  publishMavenStyle := true,
  pomExtra := (<url>https://github.com/PagerDuty/scheduler</url>
    <scm>
      <url>git@github.com:PagerDuty/scheduler.git</url>
      <connection>scm:git:git@github.com:PagerDuty/scheduler.git</connection>
    </scm>
    <developers>
      <developer>
        <id>lexn82</id>
        <name>Aleksey Nikiforov</name>
        <url>https://github.com/lexn82</url>
      </developer>
      <developer>
        <id>cdegroot</id>
        <name>Cees de Groot</name>
        <url>https://github.com/cdegroot</url>
      </developer>
      <developer>
        <id>DWvanGeest</id>
        <name>David van Geest</name>
        <url>https://github.com/DWvanGeest</url>
      </developer>
      <developer>
        <id>divtxt</id>
        <name>Div Shekhar</name>
        <url>https://github.com/divtxt</url>
      </developer>
      <developer>
        <id>jppierri</id>
        <name>Joseph Pierri</name>
        <url>https://github.com/jppierri</url>
      </developer>
    </developers>)
)

// Dependencies:
//   - scheduler and scala-api are both top-level dependencies which can be mixed in to other
//     projects (one or both, depending on whether the project provides its own interface to
//     its underlying scheduler)
//   - common has code that both depend on
//   - httpAdmin is an optional administrative HTTP interface
lazy val sharedSettings = Seq(
  organization := "com.pagerduty",
  scalaVersion := "2.11.12",
  crossScalaVersions := Seq("2.10.7", "2.11.12"),
  testOptions in IntegrationTest += Tests.Argument(TestFrameworks.ScalaTest, "-oD"),
  publishArtifact in Test := true,
  parallelExecution in IntegrationTest := false,
  resolvers := Seq(
    "bintray-pagerduty-oss-maven" at "https://dl.bintray.com/pagerduty/oss-maven",
    Resolver.defaultLocal
  )
) ++ bintraySettings

lazy val common = (project in file("common"))
  .settings(sharedSettings: _*)
  .settings(
    name := "scheduler-common",
    libraryDependencies ++= Seq(
      "com.pagerduty" %% "eris-core" % "2.0.4" exclude ("org.slf4j", "slf4j-log4j12"),
      "com.pagerduty" %% "metrics-api" % "1.3.0",
      "org.json4s" %% "json4s-jackson" % "3.3.0",
      "org.slf4j" % "slf4j-api" % "1.7.13",
      "org.slf4j" % "jul-to-slf4j" % "1.7.13",
      "org.apache.kafka" % "kafka-clients" % "1.0.0",
      "org.scalatest" %% "scalatest" % "3.0.1" % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.5.0" % "test",
      "ch.qos.logback" % "logback-classic" % "1.1.3" % "test"
    )
  )

lazy val scalaApi = (project in file("scala-api"))
  .dependsOn(common)
  .settings(sharedSettings: _*)
  .settings(
    name := "scheduler-scala-api",
    libraryDependencies ++= Seq(
      "com.pagerduty" %% "metrics-api" % "1.3.0",
      "org.scalatest" %% "scalatest" % "2.2.6" % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test",
      "ch.qos.logback" % "logback-classic" % "1.1.3" % "test"
    )
  )

lazy val scheduler = (project in file("scheduler"))
  .dependsOn(common % "it,test->test;compile->compile")
  .dependsOn(scalaApi % "it")
  .configs(IntegrationTest)
  .settings(inConfig(IntegrationTest)(scalafmtSettings))
  .settings(sharedSettings: _*)
  .settings(Defaults.itSettings: _*)
  .settings(
    name := "scheduler",
    unmanagedSourceDirectories in IntegrationTest +=
      baseDirectory.value / "src/test/scala/com/pagerduty/scheduler/specutil",
    libraryDependencies ++= {
      val kafkaConsumerVersion = "0.6.1"
      Seq(
        "com.pagerduty" %% "metrics-api" % "1.3.0",
        "com.pagerduty" %% "metrics-gauge" % "1.3.0",
        "com.pagerduty" %% "eris-dao" % "2.1.0",
        "com.pagerduty" %% "eris-dao" % "2.1.0" % "it" classifier "tests",
        "com.pagerduty" %% "kafka-consumer" % kafkaConsumerVersion,
        "com.pagerduty" %% "kafka-consumer-test-support" % kafkaConsumerVersion exclude ("org.slf4j", "slf4j-simple"),
        "com.typesafe.akka" %% "akka-actor" % "2.3.14",
        "com.typesafe.akka" %% "akka-slf4j" % "2.3.14",
        "com.typesafe.akka" %% "akka-testkit" % "2.3.14" % "it,test",
        "org.scalactic" %% "scalactic" % "2.2.6" % "it,test",
        "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "it,test",
        "org.scalatest" %% "scalatest" % "2.2.6" % "it,test",
        "ch.qos.logback" % "logback-classic" % "1.1.3" % "it,test"
      )
    }
  )

lazy val httpAdmin = (project in file("http-admin"))
  .dependsOn(common % "test->test;compile->compile")
  .dependsOn(scheduler % "it->it;test->test;compile->compile")
  .configs(IntegrationTest)
  .settings(inConfig(IntegrationTest)(scalafmtSettings))
  .settings(sharedSettings: _*)
  .settings(Defaults.itSettings: _*)
  .settings(
    name := "scheduler-http-admin",
    libraryDependencies ++= {
      val scalatraVersion = "2.4.0"
      Seq(
        "org.scalatra" %% "scalatra-json" % scalatraVersion,
        "org.scalatra" %% "scalatra" % scalatraVersion,
        "org.scalatra" %% "scalatra-json" % scalatraVersion,
        "org.eclipse.jetty" % "jetty-webapp" % "9.2.15.v20160210" % "compile",
        "org.scalatra" %% "scalatra-scalatest" % scalatraVersion % "test",
        "org.scalatest" %% "scalatest" % "2.2.6" % "test",
        "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test",
        "org.scalaj" %% "scalaj-http" % "2.3.0" % "it",
        "ch.qos.logback" % "logback-classic" % "1.1.3" % "it,test"
      )
    }
  )

lazy val root = (project in file("."))
  .settings(
    publish := {}
  )
  .aggregate(common, scalaApi, scheduler, httpAdmin)

scalafmtOnCompile in ThisBuild := true
