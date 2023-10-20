import sbtassembly.MergeStrategy

name := "generator"

libraryDependencies ++= Dependencies.console :+ Dependencies.janino

Test / javaOptions += "-Dnode.crypto.type=WAVES"

inConfig(Compile) {
  Seq(
    mainClass                    := Some("com.wavesenterprise.generator.GeneratorLauncher"),
    packageSrc / publishArtifact := false,
    packageBin / publishArtifact := false,
    packageDoc / publishArtifact := false
  )
}

addArtifact(Compile / assembly / artifact, assembly)

inTask(assembly) {
  Seq(
    test            := {},
    assemblyJarName := s"generators-${version.value}.jar",
    assemblyMergeStrategy := {
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
      case PathList("google", "protobuf", xs @ _*)              => MergeStrategy.first
      case PathList("mozilla", "public-suffix-list.txt")        => MergeStrategy.first
      case other                                                => (assembly / assemblyMergeStrategy).value(other)
    }
  )
}

publish := (publish dependsOn (Compile / assembly)).value
