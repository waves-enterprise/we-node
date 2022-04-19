package com.wavesenterprise.generator.atomic

import com.wavesenterprise.generator.privacy.NodeConfig
import com.wavesenterprise.state.{IntegerDataEntry, StringDataEntry}
import org.scalatest.{FlatSpec, Matchers}
import pureconfig.ConfigSource
import squants.information.Kilobytes

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class AtomicGeneratorSettingsSpec extends FlatSpec with Matchers {
  "AtomicGeneratorConfig" should "read values" in {
    val config = ConfigSource.string {
      s"""${AtomicGenerator.AtomicGeneratorConfigPath} {
         |  logging-level = DEBUG
         |  waves-crypto = no
         |  chain-id = T
         |  atomic-delay = 100ms
         |  parallelism = 10
         |  utx-limit = 1000
         |  max-wait-for-tx-leave-utx = 5 minutes
         |  contract {
         |    version = 1
         |    image = "localhost:5000/smart-kv"
         |    image-hash = "b48d1de58c39d2160a4b8a5a9cae90818da1212742ec1f11fba1209bed0a212c"
         |    id = "DP5MggKC8GJuLZshCVNSYwBtE6WTRtMM1YPPdcmwbuNg"
         |    create-params: [
         |      {
         |        type = string
         |        key = data
         |        value = "some string"
         |      },
         |      {
         |        type = integer
         |        key = length
         |        value = 10
         |      }
         |    ]
         |    call-params: [
         |      {
         |        type = string
         |        key = data
         |        value = "Input string size 500: BBBBBBBBBBBBBBBBBBBBBBBBBBBB"
         |      }
         |    ]
         |  }
         |  policy {
         |    lifespan = 5
         |    data-txs-count = 3
         |    data-size = 100KB
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
         |""".stripMargin
    }
    val settings = config.at(AtomicGenerator.AtomicGeneratorConfigPath).loadOrThrow[AtomicGeneratorSettings]
    settings.loggingLevel shouldBe "DEBUG"
    settings.wavesCrypto shouldBe false
    settings.chainId shouldBe "T"
    settings.atomicDelay shouldBe (100 millis)
    settings.parallelism shouldBe 10
    settings.utxLimit shouldBe 1000
    settings.maxWaitForTxLeaveUtx shouldBe (5 minutes)

    settings.contract.version shouldBe 1
    settings.contract.image shouldBe "localhost:5000/smart-kv"
    settings.contract.imageHash shouldBe "b48d1de58c39d2160a4b8a5a9cae90818da1212742ec1f11fba1209bed0a212c"
    settings.contract.id shouldBe Some("DP5MggKC8GJuLZshCVNSYwBtE6WTRtMM1YPPdcmwbuNg")
    settings.contract.createParams shouldBe List(StringDataEntry("data", "some string"), IntegerDataEntry("length", 10))
    settings.contract.callParams shouldBe List(StringDataEntry("data", "Input string size 500: BBBBBBBBBBBBBBBBBBBBBBBBBBBB"))

    settings.policy.lifespan shouldBe 5
    settings.policy.dataTxsCount shouldBe 3
    settings.policy.dataSize shouldBe Kilobytes(100)

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

  "AtomicGeneratorConfig" should "read values with missing contract id" in {
    val config = ConfigSource.string {
      s"""${AtomicGenerator.AtomicGeneratorConfigPath} {
         |  logging-level = DEBUG
         |  waves-crypto = no
         |  atomic-delay = 100ms
         |  parallelism = 10
         |  chain-id = T
         |  utx-limit = 1000
         |  max-wait-for-tx-leave-utx = 5 minutes
         |  contract {
         |    version = 1
         |    image = "localhost:5000/smart-kv"
         |    image-hash = "b48d1de58c39d2160a4b8a5a9cae90818da1212742ec1f11fba1209bed0a212c"
         |    create-params: [
         |      {
         |        type = string
         |        key = data
         |        value = "some string"
         |      },
         |      {
         |        type = integer
         |        key = length
         |        value = 10
         |      }
         |    ]
         |    call-params: [
         |      {
         |        type = string
         |        key = data
         |        value = "Input string size 500: BBBBBBBBBBBBBBBBBBBBBBBBBBBB"
         |      }
         |    ]
         |  }
         |  policy {
         |    lifespan = 5
         |    data-txs-count = 3
         |    data-size = 100KB
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
         |""".stripMargin
    }
    val settings = config.at(AtomicGenerator.AtomicGeneratorConfigPath).loadOrThrow[AtomicGeneratorSettings]
    settings.contract.id shouldBe None
  }
}
