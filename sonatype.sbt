publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishMavenStyle := true

pomExtra := (
  <url>https://github.com/PagerDuty/widerow</url>
  <licenses>
    <license>
      <name>BSD New</name>
      <url>https://opensource.org/licenses/BSD-3-Clause</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:PagerDuty/widerow.git</url>
    <connection>scm:git:git@github.com:PagerDuty/widerow.git</connection>
  </scm>
  <developers>
    <developer>
      <id>lexn82</id>
      <name>Aleksey Nikiforov</name>
      <url>https://github.com/lexn82</url>
    </developer>
  </developers>)
