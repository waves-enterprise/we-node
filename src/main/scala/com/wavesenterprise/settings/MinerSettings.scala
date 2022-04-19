package com.wavesenterprise.settings

import cats.Show
import com.wavesenterprise.mining.Miner
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

case class MinerSettings(enable: Boolean,
                         quorum: Int,
                         intervalAfterLastBlockThenGenerationIsAllowed: FiniteDuration,
                         noQuorumMiningDelay: FiniteDuration,
                         microBlockInterval: FiniteDuration,
                         minimalBlockGenerationOffset: FiniteDuration,
                         maxBlockSizeInBytes: Int,
                         maxTransactionsInMicroBlock: Int,
                         minMicroBlockAge: FiniteDuration,
                         pullingBufferSize: PositiveInt,
                         utxCheckDelay: FiniteDuration) {
  require(
    maxTransactionsInMicroBlock <= Miner.MaxTransactionsPerMicroblock,
    s"MinerSettings error: ${WESettings.configPath}.miner.max-transactions-in-micro-block must be <= ${Miner.MaxTransactionsPerMicroblock} (config value: $maxTransactionsInMicroBlock)"
  )

  require(
    requirement = utxCheckDelay >= Miner.MinUtxCheckDelay,
    message =
      s"MinerSettings error: ${WESettings.configPath}.miner.utx-check-delay must be >= ${Miner.MinUtxCheckDelay} (config value: $utxCheckDelay)"
  )
}

object MinerSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[MinerSettings] = deriveReader

  implicit val toPrintable: Show[MinerSettings] = { x =>
    import x._

    s"""
       |enable: $enable
       |quorum: $quorum
       |intervalAfterLastBlockThenGenerationIsAllowed: $intervalAfterLastBlockThenGenerationIsAllowed
       |noQuorumMiningDelay: $noQuorumMiningDelay
       |microBlockInterval: $microBlockInterval
       |minimalBlockGenerationOffset: $minimalBlockGenerationOffset
       |maxTransactionsInMicroBlock: $maxTransactionsInMicroBlock
       |minMicroBlockAge: $minMicroBlockAge
       |maxBlockSizeInBytes: $maxBlockSizeInBytes
       |pullingBufferSize: $pullingBufferSize
       |utxCheckDelay: $utxCheckDelay
     """.stripMargin
  }
}
