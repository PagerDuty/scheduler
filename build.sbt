publishArtifact in Test := true

parallelExecution in IntegrationTest := false

unmanagedSourceDirectories in IntegrationTest +=
  baseDirectory.value / "src/test/scala/com/pagerduty/scheduler/specutil"

testOptions in IntegrationTest += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

// See https://github.com/PagerDuty/eris-pd/blob/master/build.sbt#L27
resolvers in ThisBuild := Seq(
    "bintray-pagerduty-oss-maven" at "https://dl.bintray.com/pagerduty/oss-maven",
    Resolver.defaultLocal
  )

lazy val bintraySettings = Seq(
  bintrayOrganization := Some("pagerduty"),
  bintrayRepository := "oss-maven",
  licenses += ("BSD New", url("https://opensource.org/licenses/BSD-3-Clause")),
  publishMavenStyle := true,
  pomExtra := (
    <url>https://github.com/PagerDuty/scheduler</url>
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
//   scheduler and scala-api are both top-level dependencies which can be mixed in into other
//   projects (one or both, depending on whether the project provides its own interface to
//   its underlying scheduler). Common has code that both depend on. As the scheduler integration
//   tests need to enqueue, they depend on the api module as well.
lazy val sharedSettings = Seq(
  organization := "com.pagerduty",
  scalaVersion := "2.11.7",
  crossScalaVersions := Seq("2.10.4", "2.11.7")
) ++ bintraySettings

lazy val common = (project in file("common")).
  settings(sharedSettings: _*).
  settings(
    name := "scheduler-common",
    libraryDependencies ++= Seq(
      "com.pagerduty" %% "metrics-api" % "1.0.8",
      "org.json4s"   %% "json4s-jackson" % "3.3.0",
      "com.twitter" %% "util-core" % "6.22.2",
      "ch.qos.logback" % "logback-classic" % "1.1.3",
      "org.slf4j" % "slf4j-api" % "1.7.13",
      "org.slf4j" % "jul-to-slf4j" % "1.7.13",
      "org.apache.kafka" % "kafka-clients" % "0.9.0.1",
      "org.scalatest" %% "scalatest" % "2.2.6" % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test"
    )
  )

lazy val scalaApi = (project in file("scala-api")).
  dependsOn(common).
  settings(sharedSettings: _*).
  settings(
    name := "scheduler-scala-api",
    libraryDependencies ++= Seq(
      "com.pagerduty" %% "metrics-api" % "1.0.7",
      "org.scalatest" %% "scalatest" % "2.2.6" % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test"
    )
  )

lazy val root = (project in file(".")).
  dependsOn(common).
  dependsOn(scalaApi % "it").
  aggregate(common, scalaApi).
  configs(IntegrationTest).
  settings(sharedSettings: _*).
  settings(Defaults.itSettings: _*).
  settings(
    name := "scheduler",
    libraryDependencies ++= {
      val scalatraVersion = "2.4.0"
      val kafkaConsumerVersion = "0.3.2"
      Seq(
        "com.pagerduty" %% "metrics-api" % "1.0.7",
        "com.pagerduty" %% "eris-pd" % "3.0.2" exclude("org.slf4j", "slf4j-log4j12"),
        "com.pagerduty" %% "kafka-consumer" % kafkaConsumerVersion,
        "com.pagerduty" %% "kafka-consumer-test-support" % kafkaConsumerVersion  exclude("org.slf4j", "slf4j-simple"),
        "com.typesafe.akka" %% "akka-actor" % "2.3.14",
        "com.typesafe.akka" %% "akka-slf4j" % "2.3.14",
        "org.scalatra" %% "scalatra-json" % scalatraVersion,
        "org.scalatra" %% "scalatra" % scalatraVersion,
        "org.eclipse.jetty" % "jetty-webapp" % "9.2.15.v20160210" % "compile",
        "org.scalatra" %% "scalatra-json" % scalatraVersion,
        "org.json4s"   %% "json4s-jackson" % "3.3.0",
        "com.typesafe.akka" %% "akka-testkit" % "2.3.14" % "it,test",
        "org.scalactic" %% "scalactic" % "2.2.6" % "it,test",
        "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "it,test",
        "org.scalatest" %% "scalatest" % "2.2.6" % "it,test",
        "org.scalatra" %% "scalatra-scalatest" % scalatraVersion % "test",
        "org.scalaj" %% "scalaj-http" % "2.3.0" % "it"
      )
    })

