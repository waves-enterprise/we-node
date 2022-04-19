resolvers ++= Seq(
  "WE Nexus" at "https://artifacts.wavesenterprise.com/repository/we-public",
  Resolver.sbtPluginRepo("releases")
)

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

lazy val weCoreVersion: String = {
  val prop = new java.util.Properties()
  IO.load(prop, new File("project/build.properties"))
  prop.getProperty("wecore.version")
}

libraryDependencies ++= Seq(
  "com.beachape"        %% "enumeratum" % "1.5.15",
  "com.google.guava"    % "guava"       % "28.1-jre",
  "com.wavesenterprise" % "we-crypto"   % weCoreVersion
)
