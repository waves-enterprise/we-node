package com.wavesenterprise.settings

import pureconfig.ConfigSource

import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AdditionalCacheSettingsSpec extends AnyFlatSpec with Matchers {
  "RocksDBCacheSettings" should "read config string correctly" in {
    val config = ConfigSource.string {
      """
          |{
          |  max-cache-size = 100000
          |  max-rollback-depth = 2000
          |}
          |""".stripMargin
    }

    config.loadOrThrow[RocksDBCacheSettings] shouldBe RocksDBCacheSettings(100000, 2000)
  }

  "BlockIdsCacheSettings" should "read config string correctly" in {
    val config = ConfigSource.string {
      """
        |{
        |  max-size = 200
        |  expire-after = 10m
        |}
        |""".stripMargin
    }

    config.loadOrThrow[BlockIdsCacheSettings] shouldBe BlockIdsCacheSettings(200, 10 minutes)
  }
}
