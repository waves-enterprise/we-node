package com.wavesenterprise.consensus

import com.wavesenterprise.account.Address
import com.wavesenterprise.block.{Block, MicroBlock}
import com.wavesenterprise.consensus.MinerBanlistEntry.CancelledWarning
import com.wavesenterprise.settings.{BlockchainSettings, ConsensusSettings, FunctionalitySettings}
import com.wavesenterprise.state.{Blockchain, NG}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.{BlockchainUpdater, ValidationError}
import com.wavesenterprise.utils.NTP

trait Consensus {
  def blockConsensusValidation(currentTimestamp: => Long, block: Block): Either[ValidationError, ConsensusPostAction]

  def microBlockConsensusValidation(microBlock: MicroBlock): Either[ValidationError, Unit] = Right(())

  def calculatePostAction(block: Block): Either[ValidationError, ConsensusPostAction]

  def blockCanBeReplaced(currentTimestamp: => Long, newBlock: Block, lastBlock: Block): Boolean = false

  def checkExtensionRollback(blockCountToRemove: Int, lastBlock: Block): Either[ValidationError, Unit] = Right(())

  // returns empty seq for consensus which is not able to get current and next round miner
  def getCurrentAndNextMiners(): Either[ValidationError, Seq[Address]] = Right(Seq())

  protected val MaxTimeDrift: Long = 100 // millis

  protected def validateBlockVersion(height: Int, block: Block, fs: FunctionalitySettings): Either[ValidationError, Unit] = {
    Either.cond(
      if (height == 0)
        block.version == Block.LegacyGenesisBlockVersion || block.version == Block.ModernGenesisBlockVersion
      else
        block.version == Block.NgBlockVersion,
      (),
      GenericError(s"Expected block version ${Block.NgBlockVersion} for any block, except Genesis")
    )
  }
}

object Consensus {

  def apply(blockchainSettings: BlockchainSettings, blockchain: BlockchainUpdater with NG, time: NTP): Consensus = {
    blockchainSettings.consensus match {
      case ConsensusSettings.PoSSettings =>
        new PoSConsensus(blockchain, blockchainSettings)
      case poaSettings: ConsensusSettings.PoASettings =>
        new PoAConsensus(blockchain, blockchainSettings, time, poaSettings)
      case cftSettings: ConsensusSettings.CftSettings =>
        new CftConsensus(blockchain, blockchainSettings, time, cftSettings)
    }
  }
}

case class ConsensusPostActionDiff(minersBanHistory: Map[Address, MinerBanHistory], cancelledWarnings: Map[Address, Seq[CancelledWarning]])

object ConsensusPostActionDiff {
  val empty = ConsensusPostActionDiff(Map.empty, Map.empty)
}

trait ConsensusPostAction {
  def apply(blockchain: Blockchain): ConsensusPostActionDiff
}

object ConsensusPostAction {
  val NoAction: ConsensusPostAction = _ => ConsensusPostActionDiff.empty
}
