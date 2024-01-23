import sbt._

object Dependencies {

  def akkaModule(module: String): ModuleID = "com.typesafe.akka" %% s"akka-$module" % "2.6.20"

  def akkaHttpModule(module: String = ""): ModuleID = "com.typesafe.akka" %% s"akka-http${if (module.isEmpty) "" else s"-$module"}" % "10.2.0"

  def nettyModule(module: String): ModuleID = "io.netty" % s"netty-$module" % "4.1.59.Final"

  def kamonModule(module: String, v: String): ModuleID = "io.kamon" %% s"kamon-$module" % v

  val AkkaHTTP = akkaHttpModule()

  val asyncHttpClient = "org.asynchttpclient" % "async-http-client" % "2.10.5"

  lazy val weCoreVersion: String = {
    val prop = new java.util.Properties()
    IO.load(prop, new File("project/build.properties"))
    prop.getProperty("wecore.version")
  }

  lazy val weCore = Seq(
    ("com.wavesenterprise" % "we-core" % weCoreVersion)
      .exclude("com.sun.activation", "jakarta.activation")
      .exclude("com.sun.activation", "javax.activation")
      .exclude("jakarta.activation", "activation-api")
      .exclude("javax.activation", "activation")
      .exclude("io.grpc", "grpc-netty-shaded"),
    "com.wavesenterprise" % "we-test-core" % weCoreVersion % Test
  )

  lazy val wevm = "com.wavesenterprise" % "wevm_2.12" % "0.1.1-3-0cafb616bbb94a9ac5fc24a9b1c7ffa6e1254ff2-0cafb61-SNAPSHOT"

  lazy val netty = Seq("handler", "buffer", "codec", "codec-http2").map(nettyModule)

  lazy val network = netty ++ Seq(
    "org.bitlet" % "weupnp" % "0.1.4",
    // Solves an issue with kamon-influxdb
    asyncHttpClient
  )

  lazy val testKit = Seq(
    akkaModule("testkit"),
    akkaHttpModule("testkit"),
    "org.scalamock"     %% "scalamock-scalatest-support" % "3.6.0",
    "org.scalatestplus" %% "scalacheck-1-14"             % "3.1.0.0"
  )

  val dockerJavaVersion = "3.3.0"
  lazy val docker = Seq(
    "com.github.docker-java" % "docker-java-core"                  % dockerJavaVersion,
    "com.github.docker-java" % "docker-java-transport-httpclient5" % dockerJavaVersion
  )

  lazy val itKit = scalatest ++ docker ++ Seq(
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-properties" % "2.11.1",
    asyncHttpClient
  )

  lazy val serialization = Seq(
    "com.google.guava"  % "guava"                     % "28.1-jre",
    "com.typesafe.play" %% "play-json"                % "2.7.4",
    "org.julienrf"      %% "play-json-derived-codecs" % "6.0.0"
  )

  lazy val db = Seq("org.rocksdb" % "rocksdbjni" % "8.0.0")

  lazy val logging = Seq(
    "ch.qos.logback"       % "logback-classic"          % "1.2.3",
    "org.slf4j"            % "slf4j-api"                % "1.7.26",
    "org.slf4j"            % "jul-to-slf4j"             % "1.7.26",
    "net.logstash.logback" % "logstash-logback-encoder" % "4.11"
  )

  lazy val http = Seq(
    "com.fasterxml.jackson.core"   % "jackson-databind"      % "2.11.1",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.11.1",
    "com.pauldijou"                %% "jwt-play-json"        % "3.2.0",
    AkkaHTTP,
    akkaModule("discovery"),
    akkaModule("slf4j"),
    akkaModule("stream")
  )

  lazy val metrics = Seq(
    kamonModule("core", "1.1.6"),
    kamonModule("system-metrics", "1.0.1").exclude("io.kamon", "kamon-core_2.12"),
    kamonModule("influxdb", "1.0.3"),
    "org.influxdb" % "influxdb-java" % "2.16"
  ).map(_.exclude("org.asynchttpclient", "async-http-client"))

  lazy val meta  = Seq("com.chuusai" %% "shapeless" % "2.3.3")
  lazy val monix = Seq("io.monix"    %% "monix"     % "3.2.2")

  lazy val scodec    = Seq("org.scodec"  %% "scodec-core" % "1.10.3")
  lazy val fastparse = Seq("com.lihaoyi" %% "fastparse"   % "2.2.4")
  lazy val ficus     = Seq("com.iheart"  %% "ficus"       % "1.4.7")
  lazy val scorex = Seq(
    ("org.scorexfoundation" %% "scrypto" % "2.1.6")
      .exclude("ch.qos.logback", "logback-classic")
      .exclude("com.typesafe.scala-logging", "scala-logging_2.12")
      .exclude("com.google.guava", "guava")
      .exclude("org.bouncycastle", "*"),
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
  )

  lazy val bouncyCastle = Seq(
    "org.bouncycastle" % "bcprov-jdk15on" % "1.60",
    "org.bouncycastle" % "bcpkix-jdk15on" % "1.60"
  )

  lazy val commonsNet  = Seq("commons-net"        % "commons-net"   % "3.6")
  lazy val commonsLang = Seq("org.apache.commons" % "commons-lang3" % "3.8")

  lazy val scalatest = Seq("org.scalatest" %% "scalatest" % "3.1.0")

  lazy val catsCore   = Seq("org.typelevel" %% "cats-core" % "2.0.0")
  lazy val catsEffect = Seq("org.typelevel" %% "cats-effect" % "2.0.0")
  lazy val catsMtl    = Seq("org.typelevel" %% "cats-mtl-core" % "0.7.0")
  lazy val fp         = catsCore ++ catsEffect ++ catsMtl

  lazy val kindProjector = "org.spire-math" %% "kind-projector" % "0.9.10"
  lazy val enumeratum    = Seq("com.beachape" %% "enumeratum-play-json" % "1.5.16")

  private lazy val postgresql    = "org.postgresql"     % "postgresql"      % "42.2.8"
  private lazy val slick         = "com.typesafe.slick" %% "slick"          % "3.3.2"
  private lazy val slickHikariCp = "com.typesafe.slick" %% "slick-hikaricp" % "3.3.2"
  private lazy val flyway        = "org.flywaydb"       % "flyway-core"     % "5.2.4"

  private lazy val testcontainers = Seq(
    "org.testcontainers" % "testcontainers"                   % "1.16.0" % Test,
    "org.testcontainers" % "postgresql"                       % "1.16.0" % Test,
    "com.dimafeng"       %% "testcontainers-scala-scalatest"  % "0.38.0" % Test,
    "com.dimafeng"       %% "testcontainers-scala-postgresql" % "0.38.0" % Test
  )

  lazy val dbDependencies = Seq(postgresql, slick, slickHikariCp, flyway) ++ testcontainers

  lazy val awsBom = "software.amazon.awssdk" % "bom" % "2.13.2"
  lazy val awsS3 = ("software.amazon.awssdk" % "s3" % "2.13.2")
    .exclude("io.netty", "*")
    .exclude("commons-logging", "commons-logging")
    .exclude("org.apache.httpcomponents", "*")

  lazy val awsDependencies = Seq(awsBom, awsS3)

  lazy val console = Seq("com.github.scopt" %% "scopt" % "4.0.0-RC2")

  lazy val janino = "org.codehaus.janino" % "janino" % "3.0.12"

  lazy val javaplot = Seq("com.panayotis" % "javaplot" % "0.5.0" % Test)

  lazy val pureConfig = Seq(
    "com.github.pureconfig" %% "pureconfig"            % "0.12.2",
    "com.github.pureconfig" %% "pureconfig-squants"    % "0.12.2",
    "com.github.pureconfig" %% "pureconfig-enumeratum" % "0.12.2"
  )

  lazy val silencer: Seq[ModuleID] = Seq(
    compilerPlugin("com.github.ghik" % "silencer-plugin" % "1.4.3" cross CrossVersion.full),
    "com.github.ghik" % "silencer-lib" % "1.4.3" % Provided cross CrossVersion.full
  )
}
