package com.wavesenterprise.generator.atomic

import cats.Show
import cats.implicits.showInterpolator
import com.wavesenterprise.generator.privacy.NodeConfig
import com.wavesenterprise.state.DataEntry
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._
import pureconfig.module.squants._
import squants.information.Information

import scala.concurrent.duration.FiniteDuration

case class AtomicGeneratorSettings(
    loggingLevel: String,
    wavesCrypto: Boolean,
    chainId: String,
    atomicDelay: FiniteDuration,
    parallelism: Int,
    utxLimit: Int,
    maxWaitForTxLeaveUtx: FiniteDuration,
    contract: ContractSettings,
    policy: PolicySettings,
    nodes: List[NodeConfig]
) {
  require(nodes.nonEmpty, "Nodes can't be empty")
}

object AtomicGeneratorSettings {
  implicit val configReader: ConfigReader[AtomicGeneratorSettings] = deriveReader

  implicit val toPrintable: Show[AtomicGeneratorSettings] = { x =>
    import x._
    s"""
       |loggingLevel: $loggingLevel
       |wavesCrypto: $wavesCrypto
       |chainId: $chainId
       |atomicDelay: $atomicDelay
       |parallelism: $parallelism
       |utxLimit: $utxLimit
       |maxWaitForTxLeaveUtx: $maxWaitForTxLeaveUtx
       |contractSettings:
       |  ${show"$contract".replace("\n", "\n--")}
       |policySettings:
       |  ${show"$policy".replace("\n", "\n--")}
       |nodes:
       |  ${nodes.map(n => s"${show"$n"}").mkString.replace("\n", "\n--")}""".stripMargin
  }
}

case class ContractSettings(
    version: Int,
    image: String,
    imageHash: String,
    id: Option[String],
    createParams: List[DataEntry[_]],
    callParams: List[DataEntry[_]]
)

object ContractSettings {
  implicit val configReader: ConfigReader[ContractSettings] = deriveReader

  implicit val toPrintable: Show[ContractSettings] = { x =>
    import x._
    s"""
       |version: $version
       |image: $image
       |imageHash: $imageHash
       |contractId: $id
       |createParams:
       |  ${createParams.map(n => s"${show"$n"}").mkString.replace("\n", "\n--")}
       |callParams: $callParams
       |  ${callParams.map(n => s"${show"$n"}").mkString.replace("\n", "\n--")}""".stripMargin
  }
}

case class PolicySettings(
    lifespan: Int,
    dataTxsCount: Int,
    dataSize: Information
)

object PolicySettings {
  implicit val configReader: ConfigReader[PolicySettings] = deriveReader

  implicit val toPrintable: Show[PolicySettings] = { x =>
    import x._
    s"""
       |lifespan: $lifespan
       |dataTxsCount: $dataTxsCount
       |dataSize: $dataSize""".stripMargin
  }
}
