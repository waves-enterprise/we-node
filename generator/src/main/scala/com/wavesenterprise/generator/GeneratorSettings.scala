package com.wavesenterprise.generator

import cats.Show
import cats.implicits.showInterpolator
import com.wavesenterprise.OwnerCredentials
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.generator.GeneratorSettings.NodeAddress
import com.wavesenterprise.network.TrafficLogger

import java.net.{InetSocketAddress, URL}

case class GeneratorSettings(chainId: String,
                             consensusType: String,
                             wavesCrypto: Boolean,
                             ownerCredentials: OwnerCredentials,
                             accounts: AccountSettings,
                             sendTo: Seq[NodeAddress],
                             trafficLogger: TrafficLogger.Settings,
                             worker: WorkerSettings,
                             mode: Mode.Value,
                             narrow: NarrowTransactionGenerator.Settings,
                             wide: WideTransactionGenerator.Settings,
                             dynWide: DynamicWideTransactionGenerator.Settings,
                             multisig: MultisigTransactionGenerator.Settings,
                             oracle: OracleTransactionGenerator.Settings,
                             swarm: SmartGenerator.Settings,
                             dockerCall: ContractCallGenerator.Settings) {
  val addressScheme: Char                        = chainId.head
  def privateKeyAccounts: Seq[PrivateKeyAccount] = accounts.accounts
}

object GeneratorSettings {
  case class NodeAddress(networkAddress: InetSocketAddress, apiAddress: URL)

  implicit val toPrintable: Show[GeneratorSettings] = { x =>
    import x._

    val modeSettings: String = (mode match {
      case Mode.NARROW      => show"$narrow"
      case Mode.WIDE        => show"$wide"
      case Mode.DYN_WIDE    => show"$dynWide"
      case Mode.MULTISIG    => show"$multisig"
      case Mode.ORACLE      => show"$oracle"
      case Mode.SWARM       => show"$swarm"
      case Mode.DOCKER_CALL => show"$dockerCall"
    }).toString

    s"""network byte: $chainId
       |rich accounts:
       |  ${accounts.accounts.mkString("\n  ")}
       |recipient nodes:
       |  ${sendTo.mkString("\n  ")}
       |worker:
       |  ${show"$worker".replace("\n", "\n  ")}
       |mode: $mode
       |$mode settings:
       |  ${modeSettings.replace("\n", "\n  ")}
       |dockerCall:
       |  ${show"$dockerCall".replace("\n", "\n  ")}""".stripMargin
  }
}
