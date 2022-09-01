package com.wavesenterprise.settings

import pureconfig.ConfigSource

import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MinerSettingsSpecification extends AnyFlatSpec with Matchers {

  "MinerSettings" should "read values" in {
    val configSource = ConfigSource.string {
      """
        |{
        |    enable: yes
        |    quorum: 1
        |    interval-after-last-block-then-generation-is-allowed: 1d
        |    no-quorum-mining-delay = 5s
        |    micro-block-interval: 5s
        |    minimal-block-generation-offset: 500ms
        |    max-transactions-in-micro-block: 400
        |    max-block-size-in-bytes: 1048576
        |    min-micro-block-age: 3s
        |    pulling-buffer-size = 10
        |    utx-check-delay = 150ms
        |}
      """.stripMargin
    }

    val settings = configSource.loadOrThrow[MinerSettings]

    settings.enable should be(true)
    settings.quorum should be(1)
    settings.microBlockInterval should be(5.seconds)
    settings.noQuorumMiningDelay should be(5.seconds)
    settings.minimalBlockGenerationOffset should be(500.millis)
    settings.maxTransactionsInMicroBlock should be(400)
    settings.minMicroBlockAge should be(3.seconds)
    settings.pullingBufferSize.value should be(10)
    settings.utxCheckDelay shouldBe 150.millis
  }

  "MinerSettings" should "show expected error because of violated require()" in {
    val configSourceBroken = ConfigSource.string {
      """
        |{
        |    enable: yes
        |    quorum: 1
        |    interval-after-last-block-then-generation-is-allowed: 1d
        |    no-quorum-mining-delay = 5s
        |    micro-block-interval: 5s
        |    minimal-block-generation-offset: 500ms
        |    max-transactions-in-micro-block: 400
        |    max-block-size-in-bytes: 1048576
        |    min-micro-block-age: 3s
        |    pulling-buffer-size = 10
        |    utx-check-delay = 87ms // must be >= 100ms
        |}
      """.stripMargin
    }

    val exception = intercept[Exception] {
      configSourceBroken.loadOrThrow[MinerSettings]
    }

    exception.getMessage should include("utx-check-delay must be >=")
  }
}
