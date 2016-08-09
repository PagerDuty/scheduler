bintrayOrganization := Some("pagerduty")

bintrayRepository := "oss-maven"

licenses += ("BSD New", url("https://opensource.org/licenses/BSD-3-Clause"))

publishMavenStyle := true

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
