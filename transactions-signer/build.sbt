import sbtassembly.AssemblyPlugin.autoImport.assemblyMergeStrategy
import sbtassembly.MergeStrategy

name := "transactions-signer"

libraryDependencies ++= Dependencies.console ++
  Dependencies.testKit.map(_ % "test")

inConfig(Compile) {
  Seq(
    mainClass := Some("com.wavesenterprise.TxSignerApplication"),
    packageSrc / publishArtifact := false,
    packageBin / publishArtifact := false,
    packageDoc / publishArtifact := false
  )
}

addArtifact(Compile / assembly / artifact, assembly)

inTask(assembly) {
  Seq(
    test := {},
    assemblyJarName := s"transactions-signer-${version.value}.jar",
    assemblyMergeStrategy := {
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.concat
      case PathList("META-INF", "aop.xml")                      => MergeStrategy.discard
      case PathList("javax", "ws", xs @ _*)                     => MergeStrategy.discard
      case PathList("javax", "activation", xs @ _*)             => MergeStrategy.discard
      case PathList("com", "sun", "activation", xs @ _*)        => MergeStrategy.discard
      case path if path.endsWith("module-info.class")           => MergeStrategy.discard
      case other                                                => (assembly / assemblyMergeStrategy).value(other)
    }
  )
}

publish := (publish dependsOn (Compile / assembly)).value
