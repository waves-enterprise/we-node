package com.wavesenterprise.settings

import com.wavesenterprise.state.diffs.CommonValidation.MaxTimePrevBlockOverTransactionDiff
import pureconfig.ConfigSource

import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UTXSettingsSpecification extends AnyFlatSpec with Matchers {

  "UTXSettings" should "read values" in {

    val config = ConfigSource.string {
      """
        |{
        |    cleanup-interval = 10m
        |    allow-transactions-from-smart-accounts = false
        |    memory-limit = 4GiB
        |    rebroadcast-threshold = 5m
        |    rebroadcast-interval = 5m
        |  }
        |""".stripMargin
    }

    val settings = config.loadOrThrow[UtxSettings]
    settings.cleanupInterval shouldBe 10.minutes
    settings.allowTransactionsFromSmartAccounts shouldBe false
    settings.memoryLimit.toGibibytes shouldBe 4
    settings.txExpireTimeout shouldBe MaxTimePrevBlockOverTransactionDiff
    settings.rebroadcastThreshold shouldBe 5.minutes
    settings.rebroadcastInterval shouldBe 5.minutes
  }

  "UTXSettings" should "read tx timeout" in {

    val config = ConfigSource.string {
      """
        |{
        |    cleanup-interval = 10m
        |    allow-transactions-from-smart-accounts = false
        |    memory-limit = 4GiB
        |    tx-expire-timeout = 10 hours
        |    rebroadcast-threshold = 5m
        |    rebroadcast-interval = 5m
        |  }
        |""".stripMargin
    }

    val settings = config.loadOrThrow[UtxSettings]
    settings.cleanupInterval shouldBe 10.minutes
    settings.allowTransactionsFromSmartAccounts shouldBe false
    settings.memoryLimit.toGibibytes shouldBe 4
    settings.txExpireTimeout shouldBe 10.hours
  }

  "UTXSettings" should "not read tx timeout" in {

    val config = ConfigSource.string {
      """
        |{
        |    cleanup-interval = 10m
        |    allow-transactions-from-smart-accounts = false
        |    memory-limit = 4GiB
        |    tx-expire-timeout = 1 hour
        |    rebroadcast-threshold = 5m
        |    rebroadcast-interval = 5m
        |  }
        |""".stripMargin
    }

    val caught = intercept[IllegalArgumentException] {
      config.loadOrThrow[UtxSettings]
    }
    caught.getMessage shouldBe "requirement failed: txExpireTimeout param should be between 2 and 96 hours"
  }
}
