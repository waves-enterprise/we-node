import com.lightbend.sbt.SbtProguard.autoImport._
import com.typesafe.sbt.SbtGit.GitKeys.gitUncommittedChanges
import com.typesafe.sbt.git.JGit
import org.eclipse.jgit.submodule.SubmoduleWalk.IgnoreSubmoduleMode
import sbt.Keys.{credentials, sourceGenerators, _}
import sbt.internal.inc.ReflectUtilities
import sbt.librarymanagement.ivy.IvyDependencyResolution
import sbt.{Compile, Credentials, Def, Path, _}
import sbtassembly.MergeStrategy

import java.io.File
import scala.sys.process.{Process, ProcessLogger}

excludeDependencies ++= Seq(
  // workaround for https://github.com/sbt/sbt/issues/3618
  // include "jakarta.ws.rs" % "jakarta.ws.rs-api" instead
  ExclusionRule("javax.ws.rs", "javax.ws.rs-api")
)

libraryDependencies += "jakarta.ws.rs" % "jakarta.ws.rs-api" % "2.1.5"

enablePlugins(JavaServerAppPackaging, SystemdPlugin, GitVersioning)
scalafmtOnCompile in ThisBuild := true

Global / cancelable := true
Global / onChangedBuildSource := ReloadOnSourceChanges

fork in run := true
connectInput in run := true

name := "waves-enterprise"

/**
  * You have to put your credentials in a local file ~/.sbt/.credentials
  * File structure:
  *
  * realm=Sonatype Nexus Repository Manager
  * host=artifacts.wavesenterprise.com
  * username={YOUR_LDAP_USERNAME}
  * password={YOUR_LDAP_PASSWORD}
  */
credentials += {
  val envUsernameOpt = sys.env.get("nexusUser")
  val envPasswordOpt = sys.env.get("nexusPassword")

  (envUsernameOpt, envPasswordOpt) match {
    case (Some(username), Some(password)) =>
      println("Using credentials from environment for artifacts.wavesenterprise.com")
      Credentials("Sonatype Nexus Repository Manager", "artifacts.wavesenterprise.com", username, password)

    case _ =>
      val localCredentialsFile = Path.userHome / ".sbt" / ".credentials"
      println(s"Going to use ${localCredentialsFile.getAbsolutePath} as credentials for artifacts.wavesenterprise.com")
      Credentials(localCredentialsFile)
  }
}

lazy val nodeJarName = settingKey[String]("Name for assembled node jar")
nodeJarName := s"waves-enterprise-all-${version.value}.jar"

lazy val generatorJarName = settingKey[String]("Name for generator jar")
generatorJarName := s"generators-${version.value}.jar"

val nodeVersionSource = Def.task {
  // WARNING!!!
  // Please, update the fallback version every major and minor releases.
  // This version is used then building from sources without Git repository
  // In case of not updating the version nodes build from headless sources will fail to connect to newer versions
  val FallbackVersion = (1, 0, 0)

  val nodeVersionFile: File = (sourceManaged in Compile).value / "com" / "wavesenterprise" / "Version.scala"
  val versionExtractor      = """(\d+)\.(\d+)\.(\d+).*""".r
  val (major, minor, patch) = version.value match {
    case versionExtractor(ma, mi, pa) => (ma.toInt, mi.toInt, pa.toInt)
    case _                            => FallbackVersion
  }

  IO.write(
    nodeVersionFile,
    s"""package com.wavesenterprise
       |
       |object Version {
       |  val VersionString = "${version.value}"
       |  val VersionTuple = ($major, $minor, $patch)
       |}
       |""".stripMargin
  )

  Seq(nodeVersionFile)
}

normalizedName := s"${name.value}"

gitUncommittedChanges in ThisBuild := JGit(baseDirectory.value).porcelain
  .status()
  .setIgnoreSubmodules(IgnoreSubmoduleMode.ALL)
  .call()
  .hasUncommittedChanges

version in ThisBuild := {
  val suffix         = git.makeUncommittedSignifierSuffix(git.gitUncommittedChanges.value, Some("DIRTY"))
  val releaseVersion = git.releaseVersion(git.gitCurrentTags.value, git.gitTagToVersionNumber.value, suffix)
  lazy val describedExtended = git.gitDescribedVersion.value.map { described =>
    val commitHashLength                          = 7
    val (tagVersionWithoutCommitHash, commitHash) = described.splitAt(described.length - commitHashLength)
    val tagVersionWithCommitsAhead                = tagVersionWithoutCommitHash.dropRight(2)
    s"$tagVersionWithCommitsAhead-$commitHash" + suffix
  }
  releaseVersion.orElse(describedExtended).getOrElse(git.formattedDateVersion.value)
}

publishArtifact in (Compile, packageDoc) := false
publishArtifact in (Compile, packageSrc) := false
mainClass in Compile := Some("com.wavesenterprise.Application")
scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-Ywarn-unused:-implicits",
  "-Xlint",
  "-Yresolve-term-conflict:object",
  "-Ypartial-unification",
  "-language:postfixOps"
)
logBuffered := false

inThisBuild(
  Seq(
    scalaVersion := "2.12.10",
    organization := "com.wavesenterprise",
    crossPaths := false,
    scalacOptions ++= Seq("-feature",
                          "-deprecation",
                          "-language:higherKinds",
                          "-language:implicitConversions",
                          "-Ywarn-unused:-implicits",
                          "-Xlint",
                          "-Ypartial-unification")
  ))

scalaModuleInfo ~= (_.map(_.withOverrideScalaVersion(true)))

// for sbt plugins sources resolving
updateSbtClassifiers / dependencyResolution := IvyDependencyResolution((updateSbtClassifiers / ivyConfiguration).value)
resolvers ++= Seq(
  "WE Nexus" at "https://artifacts.wavesenterprise.com/repository/we-public",
  Resolver.sbtPluginRepo("releases")
)

javaOptions in run ++= Seq(
  "-XX:+IgnoreUnrecognizedVMOptions"
)

Test / fork := true
Test / javaOptions ++= Seq(
  "-XX:+IgnoreUnrecognizedVMOptions",
  "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED",
  "-Dnode.waves-crypto=true"
)
Test / parallelExecution := true

val aopMerge: MergeStrategy = new MergeStrategy {
  val name = "aopMerge"
  import scala.xml._
  import scala.xml.dtd._

  def apply(tempDir: File, path: String, files: Seq[File]): Either[String, Seq[(File, String)]] = {
    val dt                         = DocType("aspectj", PublicID("-//AspectJ//DTD//EN", "http://www.eclipse.org/aspectj/dtd/aspectj.dtd"), Nil)
    val file                       = MergeStrategy.createMergeTarget(tempDir, path)
    val xmls: Seq[Elem]            = files.map(XML.loadFile)
    val aspectsChildren: Seq[Node] = xmls.flatMap(_ \\ "aspectj" \ "aspects" \ "_")
    val weaverChildren: Seq[Node]  = xmls.flatMap(_ \\ "aspectj" \ "weaver" \ "_")
    val options: String            = xmls.map(x => (x \\ "aspectj" \ "weaver" \ "@options").text).mkString(" ").trim
    val weaverAttr                 = if (options.isEmpty) Null else new UnprefixedAttribute("options", options, Null)
    val aspects                    = new Elem(null, "aspects", Null, TopScope, false, aspectsChildren: _*)
    val weaver                     = new Elem(null, "weaver", weaverAttr, TopScope, false, weaverChildren: _*)
    val aspectj                    = new Elem(null, "aspectj", Null, TopScope, false, aspects, weaver)
    XML.save(file.toString, aspectj, "UTF-8", xmlDecl = false, dt)
    IO.append(file, IO.Newline.getBytes(IO.defaultCharset))
    Right(Seq(file -> path))
  }
}

inTask(assembly)(
  Seq(
    test := {},
    assemblyJarName := nodeJarName.value,
    assemblyMergeStrategy := {
      case PathList("META-INF", "io.netty.versions.properties")                                   => MergeStrategy.concat
      case PathList("META-INF", "aop.xml")                                                        => aopMerge
      case PathList("com", "google", "thirdparty", xs @ _*)                                       => MergeStrategy.first
      case PathList("com", "kenai", xs @ _*)                                                      => MergeStrategy.first
      case PathList("javax", "ws", xs @ _*)                                                       => MergeStrategy.first
      case PathList("jersey", "repackaged", xs @ _*)                                              => MergeStrategy.first
      case PathList("jnr", xs @ _*)                                                               => MergeStrategy.first
      case PathList("org", "aopalliance", xs @ _*)                                                => MergeStrategy.first
      case PathList("org", "jvnet", xs @ _*)                                                      => MergeStrategy.first
      case PathList("com", "sun", "activation", xs @ _*)                                          => MergeStrategy.last
      case PathList("javax", "activation", xs @ _*)                                               => MergeStrategy.last
      case PathList("jakarta", "activation", xs @ _*)                                             => MergeStrategy.last
      case path if path.endsWith("module-info.class")                                             => MergeStrategy.discard
      case "META-INF/maven/com.kohlschutter.junixsocket/junixsocket-native-common/pom.properties" => MergeStrategy.first
      case PathList("com", "google", "protobuf", xs @ _*)                                         => MergeStrategy.first
      case other                                                                                  => (assemblyMergeStrategy in assembly).value(other)
    }
  ))

inConfig(Compile)(
  Seq(
    mainClass := Some("com.wavesenterprise.Application"),
    publishArtifact in packageDoc := false,
    publishArtifact in packageSrc := false,
    sourceGenerators += nodeVersionSource
  ))

inConfig(Test)(
  Seq(
    logBuffered := false,
    parallelExecution := true,
    testListeners := Seq.empty,
    testOptions += Tests.Argument("-oIDOF", "-u", "target/test-reports"),
    testOptions += Tests.Setup({ _ =>
      sys.props("sbt-testing") = "true"
    })
  ))

// The bash scripts classpath only needs the fat jar
scriptClasspath := Seq((assemblyJarName in assembly).value)

commands += Command.command("packageAll") { state =>
  "clean" ::
    "assembly" ::
    state
}

bashScriptExtraDefines += s"""addJava "-Dnode.directory=/var/lib/${normalizedName.value}""""

val linuxScriptPattern = "bin/(.+)".r
val batScriptPattern   = "bin/([^.]+)\\.bat".r

inConfig(Universal)(
  Seq(
    mappings += (baseDirectory.value / s"node-example.conf" -> "doc/we.conf.sample"),
    mappings := {
      val fatJar = (assembly in Compile).value
      val universalMappings = mappings.value.map {
        case (file, batScriptPattern(script)) =>
          (file, s"bin/$script.bat")
        case (file, linuxScriptPattern(script)) =>
          (file, s"bin/$script")
        case other => other
      }

      // Filter all jar dependencies because they already exists in the fat jar
      val filtered = universalMappings filter {
        case (_, name) => !name.endsWith(".jar")
      }

      filtered :+ (fatJar -> ("lib/" + fatJar.getName))
    },
    javaOptions ++= Seq(
      // -J prefix is required by the bash script
      "-J-server",
      // JVM memory tuning for 2g ram
      "-J-Xms128m",
      "-J-Xmx2g",
      "-J-XX:+ExitOnOutOfMemoryError",
      // Java 9 support
      "-J-XX:+IgnoreUnrecognizedVMOptions",
      // from https://groups.google.com/d/msg/akka-user/9s4Yl7aEz3E/zfxmdc0cGQAJ
      "-J-XX:+UseG1GC",
      "-J-XX:+UseNUMA",
      "-J-XX:+AlwaysPreTouch",
      // probably can't use these with jstack and others tools
      "-J-XX:+PerfDisableSharedMem",
      "-J-XX:+ParallelRefProcEnabled",
      "-J-XX:+UseStringDeduplication"
    )
  ))

// https://stackoverflow.com/a/48592704/4050580
def allProjects: List[ProjectReference] = ReflectUtilities.allVals[Project](this).values.toList map { p =>
  p: ProjectReference
}

addCommandAlias(
  name = "compileAll",
  value = "; cleanAll; " +
    "compile; test:compile; " +
    "generator/compile; generator/test:compile; " +
    "transactionsSigner/compile; transactionsSigner/test:compile"
)

val scalafmtTestAllCommand = "scalafmtTestAll"
addCommandAlias(
  name = scalafmtTestAllCommand,
  value = "scalafmt::test; test:scalafmt::test; " +
    "generator/ scalafmt::test; generator / test:scalafmt::test; " +
    "transactionsSigner/ scalafmt::test; transactionsSigner / test:scalafmt::test; "
)

addCommandAlias(
  name = "compileAllCI",
  value = "; cleanAll; " +
    s"$scalafmtTestAllCommand; " +
    "compile; test:compile; " +
    "generator/compile; generator/test:compile; " +
    "transactionsSigner/compile; transactionsSigner/test:compile"
)

val weReleasesRepo = Some("Sonatype Nexus Repository Manager" at "https://artifacts.wavesenterprise.com/repository/we-releases")

lazy val node = project
  .in(file("."))
  .settings(
    addCompilerPlugin(Dependencies.kindProjector),
    libraryDependencies ++=
      Dependencies.weCore ++
        Dependencies.network ++
        Dependencies.db ++
        Dependencies.http ++
        Dependencies.serialization ++
        Dependencies.testKit.map(_ % "test") ++
        Dependencies.logging ++
        Dependencies.metrics ++
        Dependencies.fp ++
        Dependencies.meta ++
        Dependencies.ficus ++
        Dependencies.scorex ++
        Dependencies.commonsNet ++
        Dependencies.commonsLang ++
        Dependencies.monix.value ++
        Dependencies.docker ++
        Dependencies.enumeratum ++
        Dependencies.dbDependencies ++
        Dependencies.awsDependencies ++
        Dependencies.javaplot ++
        Dependencies.pureConfig,
    dependencyOverrides ++= Seq(
      Dependencies.AkkaHTTP
    ) ++ Dependencies.fastparse.value
  )
  .settings(
    credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
    publishTo := weReleasesRepo,
    publishArtifact in (Compile, packageSrc) := false,
    publishArtifact in (Compile, packageBin) := false,
    publishArtifact in (Compile, packageDoc) := false,
    addArtifact(artifact in (Compile, assembly), assembly),
    publish := (publish dependsOn (assembly in Compile)).value
  )

lazy val javaHomeProguardOption = Def.task[String] {
  (for {
    versionStr <- sys.props.get("java.version").toRight("failed to get system property java.version")
    javaHome   <- sys.props.get("java.home").toRight("failed to get system property java.home")
    version    <- "^(\\d+).*".r.findFirstMatchIn(versionStr).map(_.group(1)).toRight(s"java.version system property has wrong format: '$versionStr'")
  } yield {
    if (version.toInt > 8)
      s"-libraryjars $javaHome/jmods/java.base.jmod"
    else ""
  }) match {
    case Right(path) => path
    case Left(error) => sys.error(error)
  }
}

lazy val extLibrariesProguardExclusions = Def.task[Seq[String]] {
  libraryDependencies.value
    .map(_.organization.toLowerCase)
    .distinct
    .filterNot(org => org.startsWith("com.wavesenterprise") && org.startsWith("com.wavesplatform"))
    .flatMap { org =>
      Seq(
        s"-keep class $org.** { *; }",
        s"-keep interface $org.** { *; }",
        s"-keep enum $org.** { *; }"
      )
    }
}

lazy val generator = project
  .dependsOn(node)
  .dependsOn(node % "test->test; compile->compile")
  .enablePlugins(SbtProguard)
  .settings(
    Global / cancelable := true,
    fork in run := true,
    fork in Test := true,
    connectInput in run := true,
    libraryDependencies += "com.github.scopt" %% "scopt" % "3.6.0",
    assemblyJarName in assembly := s"generators-${version.value}.jar",
    mainClass in assembly := Some("com.wavesenterprise.generator.GeneratorLauncher"),
    testOptions in Test += Tests.Argument("-oIDOF", "-u", "target/generator/test-reports"),
    javaOptions in Test ++= Seq(
      "-XX:+IgnoreUnrecognizedVMOptions",
      "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED",
      "-Dnode.waves-crypto=true"
    ),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.concat
      case PathList("META-INF", "aop.xml")                      => MergeStrategy.discard
      case sign if sign.endsWith(".CP")                         => MergeStrategy.discard
      case PathList("com", "google", "thirdparty", xs @ _*)     => MergeStrategy.discard
      case PathList("com", "kenai", xs @ _*)                    => MergeStrategy.discard
      case PathList("javax", "ws", xs @ _*)                     => MergeStrategy.discard
      case PathList("jersey", "repackaged", xs @ _*)            => MergeStrategy.discard
      case PathList("jnr", xs @ _*)                             => MergeStrategy.discard
      case PathList("org", "aopalliance", xs @ _*)              => MergeStrategy.discard
      case PathList("org", "jvnet", xs @ _*)                    => MergeStrategy.discard
      case PathList("com", "sun", "activation", xs @ _*)        => MergeStrategy.discard
      case PathList("javax", "activation", xs @ _*)             => MergeStrategy.discard
      case PathList("jakarta", "activation", xs @ _*)           => MergeStrategy.discard
      case "application.conf"                                   => MergeStrategy.concat
      case path if path.endsWith("module-info.class")           => MergeStrategy.discard
      case PathList("com", "google", "protobuf", xs @ _*)       => MergeStrategy.first
      case other                                                => (assemblyMergeStrategy in assembly).value(other)
    },
    proguardInputs in Proguard := Seq((assemblyOutputPath in assembly).value),
    proguardOutputs in Proguard := Seq((proguardDirectory in Proguard).value / (assemblyJarName in assembly).value),
    proguardOptions in Proguard ++= Seq(
      "-dontnote",
      "-dontwarn",
      "-dontoptimize",
      "-dontusemixedcaseclassnames",
      javaHomeProguardOption.value,
      // to keep logback
      "-keep public class org.slf4j.** { *; }",
      "-keep public class ch.** { *; }",
      "-keep class org.bouncycastle.** { *; }",
      "-keep class scala.** { *; }",
      "-keep interface scala.** { *; }",
      "-keep enum scala.** { *; }",
      "-keep class akka.actor.** { *; }",
      "-keep interface akka.actor.** { *; }",
      "-keep enum akka.actor.** { *; }",
      """-keepnames public class * {
        |public static ** classTag();
        |}""".stripMargin,
      """-keepclasseswithmembers public class * {
        | public static void main(java.lang.String[]);
        |}""".stripMargin,
      """-keepclassmembers class * {
        |** MODULE$;
        |}""".stripMargin,
      """-keepclasseswithmembernames,includedescriptorclasses class * {
        |native <methods>;
        |}""".stripMargin,
      """-keepclassmembers,allowoptimization enum * {
        |public static **[] values();
        |public static ** valueOf(java.lang.String);
        |}""".stripMargin,
      "-keepattributes SourceFile,LineNumberTable"
    ) ++ extLibrariesProguardExclusions.value,
    javaOptions in (Proguard, proguard) := Seq("-Xmx2G"),
    proguardOptions in Proguard += ProguardOptions.keepMain("com.wavesenterprise.generator.GeneratorLauncher"),
    proguardInputFilter in Proguard := { file =>
      None
    },
    proguardVersion in Proguard := "6.2.2",
    (proguard in Proguard) := ((proguard in Proguard) dependsOn assembly).value
  )
  .settings(
    credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
    publishTo := weReleasesRepo,
    publishArtifact in (Compile, packageSrc) := false,
    publishArtifact in (Compile, packageBin) := false,
    publishArtifact in (Compile, packageDoc) := false,
    publishArtifact in (Proguard, proguard) := false,
    addArtifact(artifact in (Compile, assembly), assembly)
  )

lazy val transactionsSigner = (project in file("transactions-signer"))
  .dependsOn(node)
  .settings(
    name := "transactions-signer",
    mainClass := Some("com.wavesenterprise.TxSignerApplication"),
    libraryDependencies ++= Dependencies.console,
    testOptions in Test += Tests.Argument("-oIDOF", "-u", "target/transactions-signer/test-reports")
  )
  .settings(
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "io.netty.versions.properties")                                   => MergeStrategy.concat
      case PathList("META-INF", "aop.xml")                                                        => MergeStrategy.discard
      case PathList("META-INF", "mailcap.default")                                                => MergeStrategy.discard
      case PathList("META-INF", "mimetypes.default")                                              => MergeStrategy.discard
      case sign if sign.endsWith(".CP")                                                           => MergeStrategy.discard
      case PathList("com", "google", "thirdparty", xs @ _*)                                       => MergeStrategy.discard
      case PathList("com", "kenai", xs @ _*)                                                      => MergeStrategy.discard
      case PathList("javax", "ws", xs @ _*)                                                       => MergeStrategy.discard
      case PathList("jersey", "repackaged", xs @ _*)                                              => MergeStrategy.discard
      case PathList("jnr", xs @ _*)                                                               => MergeStrategy.discard
      case PathList("org", "aopalliance", xs @ _*)                                                => MergeStrategy.discard
      case PathList("org", "jvnet", xs @ _*)                                                      => MergeStrategy.discard
      case PathList("javax", "activation", xs @ _*)                                               => MergeStrategy.discard
      case PathList("com", "sun", "activation", xs @ _*)                                          => MergeStrategy.discard
      case "application.conf"                                                                     => MergeStrategy.concat
      case path if path.endsWith("module-info.class")                                             => MergeStrategy.discard
      case "META-INF/maven/com.kohlschutter.junixsocket/junixsocket-native-common/pom.properties" => MergeStrategy.first
      case PathList("com", "google", "protobuf", xs @ _*)                                         => MergeStrategy.first
      case other                                                                                  => (assemblyMergeStrategy in assembly).value(other)
    }
  )
  .settings(
    credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
    publishTo := weReleasesRepo,
    publishArtifact in (Compile, packageSrc) := false,
    publishArtifact in (Compile, packageBin) := false,
    publishArtifact in (Compile, packageDoc) := false,
    addArtifact(artifact in (Compile, assembly), assembly)
  )

def printMessage(msg: String): Unit = {
  println(" " + ">" * 3 + " " + msg)
}

def printMessageTask(msg: String) = Def.task {
  printMessage(msg)
}

/* ********************************************************* */

/**
  * Some convenient assembly machinery:
  *   task "prepareJars": compiles and assembles node and generator, puts resulting fat jars to cleaned 'jars' dir
  *   task "cleanJars": deletes 'jars' directory
  */
lazy val jarsDir = settingKey[File]("Jars output directory")

jarsDir := (baseDirectory in (node, Compile)).value / "jars"

lazy val movePrepareJarsArtifacts = Def.task[Unit] {
  val nodeJarName = (assemblyJarName in (node, assembly)).value
  val nodeJar     = (target in (node, Compile)).value / nodeJarName

  val generatorJarName = (assemblyJarName in (generator, assembly)).value
  val generatorJar     = (target in (generator, Compile)).value / generatorJarName

  val generatorConfigs = (resourceDirectory in (generator, Compile)).value.listFiles().filter(_.getName.endsWith(".conf"))

  IO.delete(jarsDir.value)
  IO.createDirectory(jarsDir.value)

  generatorConfigs.foreach { configFile =>
    val fileName = configFile.getName.split(File.separator).last
    IO.copyFile(configFile, jarsDir.value / (fileName + ".sample"))
  }

  IO.copyFile(nodeJar, jarsDir.value / nodeJarName)
  IO.copyFile(generatorJar, jarsDir.value / generatorJarName)
}

/* ********************************************************* */

lazy val cleanJars = taskKey[Unit]("Delete jars directory")

cleanJars := IO.delete(jarsDir.value)

/* ********************************************************* */

lazy val prepareJars = taskKey[Unit]("Package everything")

prepareJars := Def
  .sequential(
    printMessageTask("Stage 1: compile node"),
    compile in (node, Compile),
    printMessageTask("Stage 2: compile generator"),
    compile in (generator, Compile),
    printMessageTask("Stage 3: assembly node jar"),
    assembly in (node, assembly),
    printMessageTask("Stage 4: assembly generator jar"),
    assembly in (generator, assembly),
    printMessageTask("Stage 5: create jars dir and move assembled jars there"),
    movePrepareJarsArtifacts,
    printMessageTask("All done!")
  )
  .value

/* ********************************************************* */

lazy val buildAllDir = settingKey[File]("Directory for artifacts, generated with 'buildAll' task")
buildAllDir := (baseDirectory in (node, Compile)).value / "artifacts"

lazy val moveNodeFatJar = Def.task[Unit] {
  val nodeJar = (target in (node, Compile)).value / nodeJarName.value
  IO.copyFile(nodeJar, buildAllDir.value / nodeJarName.value)
}

lazy val moveGeneratorFatJar = Def.task[Unit] {
  val generatorJar = (target in (generator, Compile)).value / generatorJarName.value
  IO.copyFile(generatorJar, buildAllDir.value / generatorJarName.value)
}

lazy val moveGeneratorObfuscatedJar = Def.task[Unit] {
  val obfuscatedJar = (proguardOutputs in (generator, Proguard)).value.head
  IO.copyFile(obfuscatedJar, buildAllDir.value / generatorJarName.value)
}

lazy val recreateBuildAllDir = Def.task[Unit] {
  IO.delete(buildAllDir.value)
  IO.createDirectory(buildAllDir.value)
}

/* ********************************************************* */

/**
  * More machinery:
  *   * cleanAll - cleans all sub-projects;
  *   * buildAll - builds node and generator (to fat jars)
  */
lazy val cleanAll = taskKey[Unit]("Clean all sub-projects")

cleanAll := Def
  .sequential(
    printMessageTask("Clean node"),
    clean in node,
    printMessageTask("Clean generator"),
    clean in generator,
    printMessageTask("Clean transactionsSigner"),
    clean in transactionsSigner
  )
  .value

lazy val buildAll = taskKey[Unit]("Build node and generator (jars)")

buildAll := Def
  .sequential(
    printMessageTask("Recreate /artifacts dir"),
    recreateBuildAllDir,
    printMessageTask("Assembly node (fat jar)"),
    assembly,
    printMessageTask("Move node fat jar to /artifacts"),
    moveNodeFatJar,
    printMessageTask("Assembly generator (fat jar)"),
    assembly in generator,
    printMessageTask("Move generator fat jar to /artifacts"),
    moveGeneratorFatJar
  )
  .value

/* ********************************************************* */

lazy val errLogger = ProcessLogger(_ => (), err => sys.error(err))

lazy val dockerBuild = taskKey[Unit]("Runs Docker build")
dockerBuild := Def.task(Process(s"docker build -t wavesenterprise/node:v${version.value} .").!(errLogger))

lazy val dockerLifeCheck = taskKey[Unit]("Checks if Docker is running")
dockerLifeCheck := Def.task { Process("docker info").!(errLogger) }

lazy val release = taskKey[Unit]("Prepares artifacts for release. Since 1.1.1, it assembles and obfuscates generator jar")

release := Def
  .sequential(
    printMessageTask("Checking is Docker running"),
    dockerLifeCheck,
    printMessageTask("Recreating /artifacts dir"),
    recreateBuildAllDir,
    printMessageTask("Assembling fat jar"),
    assembly,
    printMessageTask("Building Docker image"),
    dockerBuild,
    printMessageTask("Assembling generator"),
    assembly in generator,
    printMessageTask("Obfuscating assembled generator"),
    proguard in (generator, Proguard),
    printMessageTask("Moving obfuscated generator jar to /artifacts"),
    moveGeneratorObfuscatedJar,
    printMessageTask("Done")
  )
  .value
