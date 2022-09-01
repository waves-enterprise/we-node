package com.wavesenterprise.generator

abstract class GeneratorLauncherBase extends App {
  def expectedGeneratorNames: List[String]

  def formattedGeneratorNames: String =
    expectedGeneratorNames
      .map(name => s" * $name")
      .mkString("\n")

  val generatorName =
    args.headOption
      .getOrElse(exitWithError(s"""No generator name given.
           |Please choose one of the following:
           |$formattedGeneratorNames""".stripMargin))
}

object GeneratorLauncherBase {
  val expectedGeneratorNames: List[String] = List(
    "AccountsGeneratorApp",
    "GenesisBlockGenerator",
    "ApiKeyHash"
  )
}

object GeneratorLauncher extends GeneratorLauncherBase {
  override def expectedGeneratorNames: List[String] =
    GeneratorLauncherBase.expectedGeneratorNames

  generatorName match {
    case "AccountsGeneratorApp" =>
      AccountsGeneratorApp.main(args.tail)
    case "GenesisBlockGenerator" =>
      GenesisBlockGenerator.main(args.tail)
    case "ApiKeyHash" =>
      ApiKeyHashGenerator.main(args.tail)
    case "/?" | "help" =>
      println(s"""
           |Usage:
           |  java -jar generators.jar <generator_name> <generator_arguments>
           |
           |Where <generator_name> is one of the following:
           |$formattedGeneratorNames
         """.stripMargin)
    case unknown =>
      exitWithError(s"Unknown generator $unknown. Specify one of the following:\n$formattedGeneratorNames")
  }
}
