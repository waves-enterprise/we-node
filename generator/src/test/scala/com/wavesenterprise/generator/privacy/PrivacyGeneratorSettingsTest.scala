package com.wavesenterprise.generator.privacy

import org.scalatest.{FlatSpec, Matchers}
import pureconfig.ConfigSource
import squants.information.Kilobytes

import scala.concurrent.duration._

class PrivacyGeneratorSettingsTest extends FlatSpec with Matchers {

  "PrivacyGeneratorConfig" should "read values" in {
    val config = ConfigSource.string {
      s"""${PrivacyGenerator.PrivacyGeneratorConfigPath} {
        |  logging-level = DEBUG
        |  waves-crypto = yes
        |  chain-id = T
        |  request {
        |    parallelism = 5
        |    delay = 2s
        |    data-size = 100KB
        |    utx-limit = 1000
        |  }
        |  policy {
        |    recreate-interval = 10s
        |    max-wait-for-tx-leave-utx = 2m
        |    participants-count = 2
        |  }
        |  nodes: [
        |    {
        |      api-url = "https://localhost/node-0/",
        |      privacy-api-key = "qwerty",
        |      address = "3JaaKM2QhpKqF73b4bvGdwPNiKCSi53gEwk",
        |      password = "somepass"
        |    },
        |    {
        |      api-url = "https://localhost/node-2/",
        |      privacy-api-key = "qwerty",
        |      address = "3N2cQFfUDzG2iujBrFTnD2TAsCNohDxYu8w"
        |    }
        |  ]
        |}
      """.stripMargin
    }

    val settings = config.at(PrivacyGenerator.PrivacyGeneratorConfigPath).loadOrThrow[PrivacyGeneratorSettings]
    settings.loggingLevel shouldBe "DEBUG"
    settings.wavesCrypto shouldBe true
    settings.chainId shouldBe "T"
    settings.request shouldBe RequestConfig(5, 2.seconds, Kilobytes(100), 1000)
    settings.policy shouldBe PolicyConfig(10.seconds, 2.minutes, 2)
    settings.nodes shouldBe Seq(
      NodeConfig(
        "https://localhost/node-0/",
        "qwerty",
        "3JaaKM2QhpKqF73b4bvGdwPNiKCSi53gEwk",
        Some("somepass")
      ),
      NodeConfig(
        "https://localhost/node-2/",
        "qwerty",
        "3N2cQFfUDzG2iujBrFTnD2TAsCNohDxYu8w",
        None
      )
    )
  }
}
