package com.wavesenterprise.consensus

import cats.implicits._
import com.wavesenterprise.account.Address
import com.wavesenterprise.block.Block.BlockId
import com.wavesenterprise.block.{Block, MicroBlock, TxMicroBlock, VoteMicroBlock}
import com.wavesenterprise.consensus.PoALikeConsensus.OutOfTimestampBounds.{EarlierThanBound, OlderThanBound}
import com.wavesenterprise.consensus.PoALikeConsensus.{OutOfTimestampBounds, RoundTimestamps}
import com.wavesenterprise.crypto
import com.wavesenterprise.settings.BlockchainSettings
import com.wavesenterprise.settings.ConsensusSettings.CftSettings
import com.wavesenterprise.state.NG
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.{BlockchainUpdater, ValidationError}
import com.wavesenterprise.utils.{LcgRandom, Time}

class CftConsensus(override val blockchain: BlockchainUpdater with NG,
                   override val blockchainSettings: BlockchainSettings,
                   override val time: Time,
                   val cftSettings: CftSettings)
    extends PoALikeConsensus {

  override val roundTillSyncMillis: Long = cftSettings.roundDuration.toMillis
  override val roundAndSyncMillis: Long  = roundTillSyncMillis + cftSettings.syncDuration.toMillis

  override def banDurationBlocks: Int = cftSettings.banDurationBlocks
  override def warningsForBan: Int    = cftSettings.warningsForBan
  override def maxBansPercentage: Int = cftSettings.maxBansPercentage

  val finalizationTimeoutMillis: Long = cftSettings.finalizationTimeout.toMillis
  val fullVoteSetTimeoutMillis: Long  = cftSettings.fullVoteSetTimeout.fold(0L)(_.toMillis)

  val votingMillis: Long = cftSettings.syncDuration.toMillis + finalizationTimeoutMillis

  import CftConsensus._

  def expectedValidators(parentBlockId: BlockId, timestamp: Long, currentMiner: Address): Either[GenericError, Set[Address]] = {
    val activeMinersWithoutCurrent = blockchain.miners.currentMinersSet(timestamp) - currentMiner
    val maxValidatorsCount         = cftSettings.maxValidators.value

    if (activeMinersWithoutCurrent.size > maxValidatorsCount) {
      val lastBlockCountForSeed = (math.ceil(CftRatio * maxValidatorsCount).toInt + 1).min(maxValidatorsCount)

      blockchain
        .lastBlockIds(parentBlockId, lastBlockCountForSeed)
        .map { lastBlockIds =>
          val lastBlockIdsBytes = Array.concat(lastBlockIds.view.map(_.arr).toArray: _*)
          val seed              = crypto.fastHash(lastBlockIdsBytes)
          val random            = new LcgRandom(seed)
          random.shuffle(activeMinersWithoutCurrent.toSeq).take(maxValidatorsCount).toSet
        }
        .toRight(GenericError(s"Parent block '$parentBlockId' not found"))
    } else {
      Right(activeMinersWithoutCurrent)
    }
  }

  override def blockConsensusValidation(currentTimestamp: => Long, block: Block): Either[ValidationError, ConsensusPostAction] = {
    for {
      parentBlockHeader <- blockchain
        .blockHeaderByIdWithLiquidVariations(block.reference)
        .toRight(GenericError(s"Parent block '${block.reference}' not found"))
      parentCftData <- parentBlockHeader.consensusData.asCftMaybe()
      _ <- Either.cond(
        parentCftData.isFinalized || Block.GenesisBlockVersions.contains(parentBlockHeader.version),
        (),
        GenericError(s"Parent block '${block.reference}' is not finalized")
      )
      cftData    <- block.consensusData.asCftMaybe()
      _          <- if (cftData.isFinalized) validateVotes(block, cftData.votes, s"Block '${block.uniqueId}'") else Right(())
      postAction <- super.blockConsensusValidation(currentTimestamp, block)
    } yield postAction
  }

  override def microBlockConsensusValidation(microBlock: MicroBlock): Either[ValidationError, Unit] = {
    super.microBlockConsensusValidation(microBlock) >> {
      microBlock match {
        case _: TxMicroBlock => Right(())
        case voteMicro: VoteMicroBlock =>
          for {
            lastBlock <- blockchain.lastBlock.toRight(GenericError(s"Last block not found"))
            roundTimestamps = calculateRoundTimestamps(lastBlock.timestamp)
            _ <- checkVoteMicroBlockTimestampBounds(microBlock.timestamp, roundTimestamps)
            _ <- validateVotes(lastBlock, voteMicro.votes, s"Micro-block '${microBlock.totalLiquidBlockSig}'")
          } yield ()
      }
    }
  }

  def checkVoteMicroBlockTimestampBounds(microBlockTimestamp: Long, roundTimestamps: RoundTimestamps): Either[OutOfTimestampBounds, Long] = {
    val (leftBound, rightBound) = (roundTimestamps.syncStart, roundTimestamps.roundEnd + finalizationTimeoutMillis)

    Right[OutOfTimestampBounds, Long](microBlockTimestamp)
      .ensureOr(EarlierThanBound("VoteMicroBlock", leftBound, _))(_ >= leftBound)
      .ensureOr(OlderThanBound("VoteMicroBlock", rightBound, _))(_ <= rightBound)
  }

  private def validateVotes(block: Block, votes: Seq[Vote], validationEntity: String): Either[GenericError, Unit] = {
    val roundEnd = calculateRoundTimestamps(block.timestamp).roundEnd
    expectedValidators(block.reference, roundEnd, block.sender.toAddress).flatMap { validators =>
      val expectedVotingHash = block.votingHash()
      val voteByAddress      = votes.view.filter(_.blockVotingHash == expectedVotingHash).map(vote => vote.sender.toAddress -> vote).toMap

      val missingValidatorAddresses = validators.filterNot(validator => voteByAddress.get(validator).exists(_.signatureValid()))
      val satisfaction              = (validators.size - missingValidatorAddresses.size + 1).toDouble / (validators.size + 1)

      log.debug(s"$validationEntity voting satisfaction '$satisfaction', expected validators: ${validators.mkString("'", "', '", "'")}")

      Either.cond(
        satisfaction > CftRatio,
        (),
        GenericError(
          s"$validationEntity does not contain the required percent of votes '$CftRatio', current - '$satisfaction'. " +
            s"Current votes: ${votes.mkString("'", "', '", "'")}, missing validator votes: ${missingValidatorAddresses.mkString("'", "', '", "'")}")
      )
    }
  }

  override def blockCanBeReplaced(currentTimestamp: => Long, newBlock: Block, lastBlock: Block): Boolean = {
    val nextRoundStart = nextRoundTimestamp(lastBlock.timestamp)

    lastBlock.reference == newBlock.reference &&
      currentTimestamp >= nextRoundStart &&
      newBlock.timestamp >= nextRoundStart &&
      lastBlock.consensusData.asCftMaybe().exists(_.isNotFinalized)
  }

  override def checkExtensionRollback(blocksCountToRemove: Int, lastBlock: Block): Either[ValidationError, Unit] = {
    val lastBlockNotFinalized = lastBlock.consensusData.asCftMaybe().exists(_.isNotFinalized)
    val allowedRollbackDepth  = 1 + (if (lastBlockNotFinalized) 1 else 0)

    Either.cond(
      blocksCountToRemove <= allowedRollbackDepth,
      (),
      GenericError(s"CFT consensus does not allow rollback more than '$allowedRollbackDepth' block, current depth '$blocksCountToRemove'")
    )
  }
}

object CftConsensus {
  val CftRatio: Double = 0.5
}
