package com.wavesenterprise.generator

import com.wavesenterprise.generator.atomic.AtomicGenerator
import com.wavesenterprise.generator.privacy.PrivacyGenerator

object GeneratorLauncher {
  def main(args: Array[String]): Unit = {
    val expectedGeneratorNames =
      "AccountsGeneratorApp" ::
        "GenesisBlockGenerator" ::
        "GrantRolesApp" ::
        "TransactionsGeneratorApp" ::
        "ApiKeyHash" ::
        "StateGenerator" ::
        "PrivacyGenerator" :: Nil
    val formattedGeneratorNames = expectedGeneratorNames.map(name => s" * $name").mkString("\n")

    val generatorName = args.headOption
      .getOrElse(exitWithError(s"""No generator name given.
             |Please choose one of the following:
             |$formattedGeneratorNames""".stripMargin))

    generatorName match {
      case "AccountsGeneratorApp" =>
        AccountsGeneratorApp.main(args.tail)
      case "GenesisBlockGenerator" =>
        GenesisBlockGenerator.main(args.tail)
      case "GrantRolesApp" =>
        GrantRolesApp.main(args.tail)
      case "TransactionsGeneratorApp" =>
        TransactionsGeneratorApp.main(args.tail)
      case "ApiKeyHash" =>
        ApiKeyHashGenerator.main(args.tail)
      case "StateGenerator" =>
        StateGenerator.main(args.tail)
      case "PrivacyGenerator" =>
        PrivacyGenerator.main(args.tail)
      case "AtomicGenerator" =>
        AtomicGenerator.main(args.tail)
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
}
