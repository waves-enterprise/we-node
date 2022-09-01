package com.wavesenterprise.settings

import java.io.File

import pureconfig.ConfigSource

import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AnchoringSettingsSpecification extends AnyFlatSpec with Matchers {

  "AncoringSettings" should "disable" in {
    val configSource = ConfigSource.string {
      """
        |{
        |   enable = no
        |}
        """.stripMargin
    }

    val config = configSource.loadOrThrow[AnchoringSettings]
    config shouldBe AnchoringDisableSettings
  }

  "AnchoringSettings with api-key auth" should "height-range" in {
    val configSource = ConfigSource.string {
      """
        |{
        |      enable = yes
        |      height-condition {
        |         range = 200
        |         above = 6
        |      }
        |      threshold = 100
        |
        |      targetnet {
        |
        |        auth.type = "api-key"
        |
        |        scheme-byte = "F"
        |        node-address = "http://0.0.0.0:6862"
        |        node-recipient-address = "2gN2rdzpLJWV9T5kbp…r52ip41KRBxUYEiJ"
        |        private-key-password = "targetnet-priv-key-password"
        |
        |        wallet {
        |          file = "{user.home}/anchoring/wallet/"
        |          password = "some string as password"
        |        }
        |
        |        fee = 10000
        |      }
        |
        |      sidechain-fee = 666
        |
        |      tx-mining-check-delay = 5 seconds
        |      tx-mining-check-count = 20
        |}
      """.stripMargin
    }

    val settingsFromConfig = configSource.loadOrThrow[AnchoringSettings].asInstanceOf[AnchoringEnableSettings]

    val expectedConfig = AnchoringEnableSettings(
      HeightCondition(200, 6),
      threshold = 100,
      targetnet = TargetnetSettings(
        schemeByte = "F",
        nodeAddress = "http://0.0.0.0:6862",
        auth = TargetnetAuthSettings.NoAuth,
        nodeRecipientAddress = "2gN2rdzpLJWV9T5kbp…r52ip41KRBxUYEiJ",
        privateKeyPassword = Some("targetnet-priv-key-password"),
        wallet = WalletSettings(Some(new File("{user.home}/anchoring/wallet/")), "some string as password"),
        fee = 10000L
      ),
      sidechainFee = 666L,
      txMiningCheckDelay = Some(5 seconds),
      txMiningCheckCount = Some(20)
    )

    settingsFromConfig shouldEqual expectedConfig
  }

  "AnchoringSettings with auth-service auth" should "height-range" in {
    val configSource = ConfigSource.string {
      """
        |{
        |      enable = yes
        |      height-condition {
        |         range = 100
        |         above = 6
        |      }
        |      threshold = 100
        |
        |      targetnet {
        |        auth {
        |          type = "oauth2"
        |          authorization-token = "xxxx"
        |          authorization-service-url = "http://localhost:3000"
        |          token-update-interval = "7 minutes"
        |        }
        |
        |        scheme-byte = "F"
        |        node-address = "http://0.0.0.0:6862"
        |        node-recipient-address = "2gN2rdzpLJWV9T5kbp…r52ip41KRBxUYEiJ"
        |        private-key-password = "targetnet-priv-key-password"
        |
        |        wallet {
        |          file = "{user.home}/anchoring/wallet/"
        |          password = "some string as password"
        |        }
        |
        |        fee = 10000
        |      }
        |      sidechain-fee = 666
        |
        |      tx-mining-check-delay = 5 seconds
        |      tx-mining-check-count = 20
        |}
      """.stripMargin
    }

    val settingsFromConfig = configSource.loadOrThrow[AnchoringSettings].asInstanceOf[AnchoringEnableSettings]

    val expectedConfig = AnchoringEnableSettings(
      HeightCondition(100, 6),
      threshold = 100,
      targetnet = TargetnetSettings(
        "F",
        "http://0.0.0.0:6862",
        TargetnetAuthSettings.Oauth2("xxxx", "http://localhost:3000", 7 minutes),
        "2gN2rdzpLJWV9T5kbp…r52ip41KRBxUYEiJ",
        Some("targetnet-priv-key-password"),
        WalletSettings(Some(new File("{user.home}/anchoring/wallet/")), "some string as password"),
        10000L
      ),
      sidechainFee = 666L,
      txMiningCheckDelay = Some(5 seconds),
      txMiningCheckCount = Some(20)
    )

    settingsFromConfig.targetnet.auth shouldEqual expectedConfig.targetnet.auth
  }
}
