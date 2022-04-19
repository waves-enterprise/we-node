package com.wavesenterprise.settings

import org.scalatest.{FlatSpec, Matchers}
import pureconfig.ConfigSource

class WalletSettingsSpecification extends FlatSpec with Matchers {

  "WalletSettings" should "read values from config" in {

    val configSource = buildSourceBasedOnDefault {
      ConfigSource.string {
        """node.wallet {
          |  password: "some string as password"
          |}""".stripMargin
      }
    }

    val settings = configSource.at("node.wallet").loadOrThrow[WalletSettings]

    settings.password should be("some string as password")
  }
}
