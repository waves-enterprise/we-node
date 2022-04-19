package com.wavesenterprise.settings

import com.wavesenterprise.account.Address
import com.wavesenterprise.crypto
import com.wavesenterprise.db.WithAddressSchema
import com.wavesenterprise.state.ByteStr
import org.scalatest.{FlatSpec, Matchers}
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderException

import scala.concurrent.duration._

class BlockchainSettingsSpecification extends FlatSpec with Matchers with WithAddressSchema {

  private lazy val pk1         = crypto.generatePublicKey
  private lazy val pk2         = crypto.generatePublicKey
  private lazy val pubKeyStr1  = pk1.publicKeyBase58
  private lazy val addressStr1 = pk1.address
  private lazy val pubKeyStr2  = pk2.publicKeyBase58
  private lazy val addressStr2 = pk2.address
  private lazy val defaultConfig = buildSourceBasedOnDefault {
    ConfigSource.string {
      s"""
         | node.blockchain {
         |    type = CUSTOM
         |    consensus.type = pos
         |    custom {
         |      address-scheme-character = "C"
         |      functionality {
         |        feature-check-blocks-period = 10000
         |        blocks-for-feature-activation = 9000
         |        pre-activated-features {
         |          19 = 100
         |          20 = 200
         |        }
         |      }
         |      genesis {
         |        block-timestamp = 1559260800000
         |        genesis-public-key-base-58: "21xNXVTym4Lyn5h4Lk5teFYJV7aNMVH5bfBTQuTQfnK3hrm5VZHbnWuDFBgLJWEDPdvbj39wGtyQsTubHJSaxjGu"
         |        signature = "BASE58BLKSGNATURE"
         |        initial-balance = 100000000000000
         |        initial-base-target = 153722867
         |        average-block-delay = 60s
         |        genesis-public-key-base-58 = "BASE58PK"
         |        transactions = [
         |          {recipient = "$addressStr1", amount = 50000000000001},
         |          {recipient = "$addressStr2", amount = 49999999999999}
         |        ]
         |        network-participants = [
         |         {public-key: "$pubKeyStr1", roles: [permissioner, miner]}
         |         {public-key: "$pubKeyStr2", roles: [miner]}
         |        ]
         |      }
         |    }
         |  }
         |""".stripMargin
    }
  }

  "BlockchainSettings" should "read custom values" in withAddressSchema('C') {
    val configSource = defaultConfig.at("node.blockchain")
    val settings     = configSource.loadOrThrow[BlockchainSettings]

    settings.custom.addressSchemeCharacter should be('C')
    settings.consensus should be(ConsensusSettings.PoSSettings)

    settings.custom.functionality.featureCheckBlocksPeriod should be(10000)
    settings.custom.functionality.blocksForFeatureActivation should be(9000)
    settings.custom.functionality.preActivatedFeatures should be(Map(19 -> 100, 20 -> 200))

    settings.custom.genesis shouldBe a[PlainGenesisSettings]

    val genesisSettings = settings.custom.genesis.toPlainSettings
    genesisSettings shouldBe 'right
    val plainGenesisSettings = genesisSettings.right.get
    plainGenesisSettings.blockTimestamp should be(GenesisSettings.mainnetStartDate)
    plainGenesisSettings.signature should be(ByteStr.decodeBase58("BASE58BLKSGNATURE").toOption)
    plainGenesisSettings.initialBalance should be(WestAmount(100000000000000L))
    plainGenesisSettings.initialBaseTarget should be(153722867)
    plainGenesisSettings.averageBlockDelay should be(60.seconds)
    val address1 = Address.fromString(addressStr1).right.get
    val address2 = Address.fromString(addressStr2).right.get
    plainGenesisSettings.transactions should be(
      Seq(GenesisTransactionSettings(address1, WestAmount(50000000000001L)), GenesisTransactionSettings(address2, WestAmount(49999999999999L))))
    plainGenesisSettings.networkParticipants.map(_.publicKey) should be(Seq(pubKeyStr1, pubKeyStr2))
    plainGenesisSettings.networkParticipants.find(_.publicKey == pubKeyStr1).map(_.roles) should be(Some(Seq("permissioner", "miner")))
    plainGenesisSettings.networkParticipants.find(_.publicKey == pubKeyStr2).map(_.roles) should be(Some(Seq("miner")))
  }

  "BlockchainSettings" should "throw IllegalArgumentException if can't read some values from config" in withAddressSchema('C') {
    val customConfig = ConfigSource.string("node.blockchain { custom.genesis.initial-balance = 99999999999999999999999999999999999 }")
    val config       = customConfig.withFallback(defaultConfig).at("node.blockchain")

    the[ConfigReaderException[_]] thrownBy config.loadOrThrow[BlockchainSettings] should have message
      """Cannot convert configuration to a com.wavesenterprise.settings.BlockchainSettings. Failures are:
        |  at 'node.blockchain.custom.genesis.initial-balance':
        |    - Cannot convert '99999999999999999999999999999999999' to com.wavesenterprise.settings.WestAmount: java.lang.IllegalArgumentException: GenericError(WEST amounts greater than 10 billion are not allowed).
        |""".stripMargin
  }

  "FunctionalitySettings" should "accept unset pre-activated-features map" in {
    val inputConfig = ConfigSource.string("""{
                        |        feature-check-blocks-period = 10000
                        |        blocks-for-feature-activation = 9000
                        |      }""".stripMargin)

    val readSettings = inputConfig.loadOrThrow[FunctionalitySettings]

    readSettings.preActivatedFeatures shouldBe Map.empty
  }
}
