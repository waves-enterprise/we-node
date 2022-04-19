package com.wavesenterprise.settings

import cats.Show
import cats.implicits.showInterpolator
import com.wavesenterprise.account.Address
import com.wavesenterprise.database.snapshot.ConsensualSnapshotSettings
import com.wavesenterprise.settings.dockerengine.DockerEngineSettings
import com.wavesenterprise.settings.privacy.PrivacySettings
import com.wavesenterprise.{ApplicationInfo, NodeVersion, Version}
import pureconfig._
import pureconfig.generic.semiauto.deriveReader

case class WESettings(
    directory: String,
    dataDirectory: String,
    snapshotDirectory: String,
    ownerAddress: Address,
    privacy: PrivacySettings,
    license: LicenseSettings,
    ntp: NtpSettings,
    network: NetworkSettings,
    wallet: WalletSettings,
    blockchain: BlockchainSettings,
    miner: MinerSettings,
    api: ApiSettings,
    synchronization: SynchronizationSettings,
    utx: UtxSettings,
    features: FeaturesSettings,
    anchoring: AnchoringSettings,
    dockerEngine: DockerEngineSettings,
    additionalCache: AdditionalCacheSettings,
    pki: Option[PkiSettings],
    consensualSnapshot: ConsensualSnapshotSettings,
    healthCheck: HealthCheckSettings
) {
  def applicationInfo(): ApplicationInfo =
    ApplicationInfo(
      VersionConstants.ApplicationName + blockchain.custom.addressSchemeCharacter,
      NodeVersion(Version.VersionTuple),
      blockchain.consensus.consensusType.toString,
      network.finalNodeName,
      network.finalNonce,
      network.maybeDeclaredSocketAddress
    )
}

object WESettings extends WEConfigReaders {

  val configPath: String = "node"

  implicit val configReader: ConfigReader[WESettings] = deriveReader

  implicit val toPrintable: Show[WESettings] = { x =>
    import x._

    s"""
       |directory:         $directory
       |dataDirectory:     $dataDirectory
       |snapshotDirectory: $snapshotDirectory
       |ownerAddress:      $ownerAddress
       |licenseSettings:
       |  ${show"$license".replace("\n", "\n--")}
       |ntpSettings:
       |  ${show"$ntp".replace("\n", "\n--")}
       |privacySettings:
       |  ${show"$privacy".replace("\n", "\n--")}
       |networkSettings:
       |  ${show"$network".replace("\n", "\n--")}
       |walletSettings:
       |  ${show"$wallet".replace("\n", "\n--")}
       |blockchainSettings:
       |  ${show"$blockchain".replace("\n", "\n--")}
       |minerSettings:
       |  ${show"$miner".replace("\n", "\n--")}
       |restAPISettings:
       |  ${show"$api".replace("\n", "\n--")}
       |synchronizationSettings:
       |  ${show"$synchronization".replace("\n", "\n--")}
       |utxSettings:
       |  ${show"$utx".replace("\n", "\n--")}
       |featuresSettings:
       |  ${show"$features".replace("\n", "\n--")}
       |anchoringSettings:
       |  ${show"$anchoring".replace("\n", "\n--")}
       |dockerEngineSettings:
       |  ${show"$dockerEngine".replace("\n", "\n--")}
       |additionalCache:
       |  ${show"$additionalCache".replace("\n", "\n--")}
       |consensualSnapshot:
       |  ${show"$consensualSnapshot".replace("\n", "\n--")}
       |healthCheck:
       |  ${show"$healthCheck".replace("\n", "\n--")}
       """.stripMargin
  }
}
