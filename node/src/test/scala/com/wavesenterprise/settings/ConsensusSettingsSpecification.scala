package com.wavesenterprise.settings

import pureconfig.ConfigSource

import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConsensusSettingsSpecification extends AnyFlatSpec with Matchers {

  "ConsensusSettings" should "read PoS consensus settings" in {
    val config = ConfigSource.string {
      """
        |{
        |   type = pos
        |}
        |""".stripMargin
    }

    val consensusSettings = config.loadOrThrow[ConsensusSettings]

    consensusSettings shouldBe ConsensusSettings.PoSSettings
  }

  it should "read PoA consensus settings" in {
    val config = ConfigSource.string {
      """
        |{
        |   type = poa
        |   round-duration = 60s
        |   sync-duration = 15s
        |   ban-duration-blocks = 100
        |   warnings-for-ban = 3
        |   max-bans-percentage = 50
        |}
        |""".stripMargin
    }

    val consensusSettings = config.loadOrThrow[ConsensusSettings]

    consensusSettings shouldBe ConsensusSettings.PoASettings(
      roundDuration = 60.seconds,
      syncDuration = 15.seconds,
      banDurationBlocks = 100,
      warningsForBan = 3,
      maxBansPercentage = 50
    )
  }
}
