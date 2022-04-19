package com.wavesenterprise.consensus

import com.wavesenterprise.settings.BlockchainSettings
import com.wavesenterprise.settings.ConsensusSettings.PoASettings
import com.wavesenterprise.state.NG
import com.wavesenterprise.utils.ScorexLogging

class PoAConsensus(override val ng: NG, override val blockchainSettings: BlockchainSettings, poaSettings: PoASettings)
    extends PoALikeConsensus
    with ScorexLogging {

  override val roundTillSyncMillis: Long = poaSettings.roundDuration.toMillis
  override val roundAndSyncMillis: Long  = roundTillSyncMillis + poaSettings.syncDuration.toMillis

  override def banDurationBlocks: Int = poaSettings.banDurationBlocks
  override def warningsForBan: Int    = poaSettings.warningsForBan
  override def maxBansPercentage: Int = poaSettings.maxBansPercentage
}
