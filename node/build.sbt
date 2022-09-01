import sbt.Tests.Group
import sbtassembly.MergeStrategy

import java.io.File

name := "node"

inConfig(Compile) {
  Seq(
    mainClass := Some("com.wavesenterprise.Application"),
    packageSrc / publishArtifact := false,
    packageBin / publishArtifact := false,
    packageDoc / publishArtifact := false
  )
}

addArtifact(Compile / assembly / artifact, assembly)
publish := (publish dependsOn (Compile / assembly)).value

addCompilerPlugin(Dependencies.kindProjector)

Test / javaOptions += "-Dnode.crypto.type=WAVES"

val nodeVersionSource = Def.task {
  val FallbackVersion = (1, 0, 0)

  val nodeVersionFile: File = (Compile / sourceManaged).value / "com" / "wavesenterprise" / "Version.scala"
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

Compile / sourceGenerators += nodeVersionSource

inTask(assembly) {
  Seq(
    test := {},
    assemblyJarName := s"node-${version.value}.jar",
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
      case other                                                                                  => (assembly / assemblyMergeStrategy).value(other)
    }
  )
}

val aopMerge: MergeStrategy = new MergeStrategy {
  import scala.xml._
  import scala.xml.dtd._

  val name = "aopMerge"
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

resolvers ++= Seq(
  "WE Nexus Repository Manager" at "https://artifacts.wavesenterprise.com/repository/we-releases",
  "WE Nexus Snapshots Repository Manager" at "https://artifacts.wavesenterprise.com/repository/we-snapshots"
)

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
    Dependencies.monix ++
    Dependencies.docker ++
    Dependencies.enumeratum ++
    Dependencies.dbDependencies ++
    Dependencies.awsDependencies ++
    Dependencies.javaplot ++
    Dependencies.pureConfig ++
    Dependencies.silencer

dependencyOverrides ++=
  Seq(Dependencies.AkkaHTTP) ++
    Dependencies.fastparse
