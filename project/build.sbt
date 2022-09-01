resolvers ++= Seq(
  "Sonatype Nexus Repository Manager" at "https://artifacts.wavesenterprise.com/repository/we-releases",
  "Sonatype Nexus Snapshots Repository Manager" at "https://artifacts.wavesenterprise.com/repository/we-snapshots"
)

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

lazy val weCoreVersion: String = {
  val prop = new java.util.Properties()
  IO.load(prop, new File("project/build.properties"))
  prop.getProperty("wecore.version")
}

libraryDependencies += "com.wavesenterprise" % "we-crypto" % weCoreVersion
