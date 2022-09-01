package com.wavesenterprise.settings

import pureconfig.ConfigSource
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WalletSettingsSpecification extends AnyFlatSpec with Matchers {

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
