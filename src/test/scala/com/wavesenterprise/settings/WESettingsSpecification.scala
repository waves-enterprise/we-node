package com.wavesenterprise.settings

import java.io.File

import com.wavesenterprise.crypto
import org.scalatest.{FlatSpec, Matchers}
import pureconfig.ConfigSource

class WESettingsSpecification extends FlatSpec with Matchers {

  private val home = System.getProperty("user.home")

  "WESettings" should s"read values from default config" in {

    val configSource = buildSourceBasedOnDefault {
      ConfigSource.string {
        s"""node {
             |  owner-address = ${crypto.generatePublicKey.toAddress}
             |}
             """.stripMargin
      }
    }

    val settings = configSource.at(WESettings.configPath).loadOrThrow[WESettings]

    settings.directory should be(home + "/node")
    settings.network should not be null
    settings.wallet should not be null
    settings.blockchain should not be null
    settings.miner should not be null
    settings.api should not be null
    settings.synchronization should not be null
    settings.utx should not be null
    settings.api.auth should not be null
  }

  "WESettings" should "resolve folders correctly" in {

    val configSource = buildSourceBasedOnDefault {
      ConfigSource.string {
        s"""node {
           |  owner-address = ${crypto.generatePublicKey.toAddress}
           |  directory = "/xxx"
           |  data-directory = "/xxx/data"
           |  ntp.servers = ["example.com"]
         |}""".stripMargin
      }
    }

    val settings = configSource.at(WESettings.configPath).loadOrThrow[WESettings]

    settings.directory should be("/xxx")
    settings.dataDirectory should be("/xxx/data")
    settings.ntp.servers should be(Seq("example.com"))
    settings.network.file should be(Some(new File("/xxx/peers.dat")))
    settings.wallet.file should be(Some(new File("/xxx/wallet/wallet.dat")))
  }

}
