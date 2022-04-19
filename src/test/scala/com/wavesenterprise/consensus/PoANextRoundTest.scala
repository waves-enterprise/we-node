package com.wavesenterprise.consensus

import com.wavesenterprise.settings.{ConsensusSettings, PlainGenesisSettings, _}
import com.wavesenterprise.state.NG
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, PropSpec}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._

class PoANextRoundTest extends PropSpec with ScalaCheckPropertyChecks with Matchers with MockFactory {

  val roundLengthDuration: FiniteDuration = 60.seconds
  val syncTimeDuration: FiniteDuration    = 15.seconds
  val banDurationBlocks: Int              = 100
  val warningsForBan: Int                 = 3
  val maxBansPercentage: Int              = 50
  val fullRoundDuration: FiniteDuration   = roundLengthDuration + syncTimeDuration
  val genesisTimestamp                    = GenesisSettings.minTimestamp
  val genesisSettings: GenesisSettings =
    BlockchainSettings.unitTestsConfigSource
      .at("custom.genesis")
      .loadOrThrow[GenesisSettings] match {
      case plain: PlainGenesisSettings                => plain.copy(blockTimestamp = genesisTimestamp)
      case snapshotBase: SnapshotBasedGenesisSettings => snapshotBase.copy(blockTimestamp = genesisTimestamp)
    }
  val blockchainSettings: BlockchainSettings = {
    val testConfig = BlockchainSettings.unitTestsConfigSource.loadOrThrow[BlockchainSettings]
    testConfig.copy(custom = testConfig.custom.copy(genesis = genesisSettings))
  }
  val poaSettings        = ConsensusSettings.PoASettings(roundLengthDuration, syncTimeDuration, banDurationBlocks, warningsForBan, maxBansPercentage)
  val blockchainMock: NG = mock[NG]

  val poa = new PoAConsensus(blockchainMock, blockchainSettings, poaSettings)

  property("nextRoundTimestamp() returns expected timestamp") {
    forAll(Gen.chooseNum(1, fullRoundDuration.toMillis)) { offset =>
      poa.nextRoundTimestamp(genesisTimestamp + fullRoundDuration.toMillis - offset) shouldBe (genesisTimestamp + fullRoundDuration.toMillis)
      poa.nextRoundTimestamp(genesisTimestamp + fullRoundDuration.toMillis) shouldBe (genesisTimestamp + 2 * fullRoundDuration.toMillis)
    }
  }
}
