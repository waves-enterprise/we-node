package com.wavesenterprise.generator.privacy

import cats.Show
import cats.implicits.showInterpolator
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._
import pureconfig.module.squants._
import squants.information.Information

import scala.concurrent.duration.FiniteDuration

case class PrivacyGeneratorSettings(
    loggingLevel: String,
    wavesCrypto: Boolean,
    chainId: String,
    request: RequestConfig,
    policy: PolicyConfig,
    nodes: Seq[NodeConfig]
) {
  require(nodes.size >= policy.participantsCount, "Nodes count should be more or equal to 'policy.participants-count' param")
}

object PrivacyGeneratorSettings {
  implicit val configReader: ConfigReader[PrivacyGeneratorSettings] = deriveReader

  implicit val toPrintable: Show[PrivacyGeneratorSettings] = { x =>
    import x._
    s"""
       |loggingLevel: $loggingLevel
       |request:
       |  ${show"$request".replace("\n", "\n--")}
       |policy:
       |  ${show"$policy".replace("\n", "\n--")}
       |nodes:
       |  ${nodes.map(n => s"${show"$n"}").mkString.replace("\n", "\n--")}
      """.stripMargin
  }
}

case class RequestConfig(parallelism: Int, delay: FiniteDuration, dataSize: Information, utxLimit: Int)

object RequestConfig {
  implicit val configReader: ConfigReader[RequestConfig] = deriveReader

  implicit val toPrintable: Show[RequestConfig] = { x =>
    import x._
    s"""
       |parallelism: $parallelism
       |delay: $delay
       |dataSize: $dataSize
       |utxLimit: $utxLimit
      """.stripMargin
  }
}

case class PolicyConfig(recreateInterval: FiniteDuration, maxWaitForTxLeaveUtx: FiniteDuration, participantsCount: Int) {
  require(participantsCount > 0, "'node.participants-count' config value should be more than 0 and less or equal to nodes count")
}

object PolicyConfig {
  implicit val configReader: ConfigReader[PolicyConfig] = deriveReader

  implicit val toPrintable: Show[PolicyConfig] = { x =>
    import x._
    s"""
       |recreateInterval: $recreateInterval
       |maxWaitForTxLeaveUtx: $maxWaitForTxLeaveUtx
       |participantsCount: $participantsCount
      """.stripMargin
  }
}

case class NodeConfig(apiUrl: String, privacyApiKey: String, address: String, password: Option[String])

object NodeConfig {
  implicit val configReader: ConfigReader[NodeConfig] = deriveReader

  implicit val toPrintable: Show[NodeConfig] = { x =>
    import x._
    s"""
       |apiUrl: $apiUrl
       |privacyApiKey: $privacyApiKey
       |address: $address
       |${password.map(pwd => s"password: $pwd").getOrElse("")}
      """.stripMargin
  }
}
