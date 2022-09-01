package com.wavesenterprise.consensus

import cats.implicits._
import com.wavesenterprise.account.Address
import com.wavesenterprise.block.{Block, MicroBlock, TxMicroBlock}
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.features.FeatureProvider.FeatureProviderExt
import com.wavesenterprise.settings.BlockchainSettings
import com.wavesenterprise.state.NG
import com.wavesenterprise.transaction.ValidationError.{BlockFromFuture, GenericError}
import com.wavesenterprise.transaction.{BlockchainUpdater, ValidationError}
import com.wavesenterprise.utils.{ScorexLogging, Time}

import scala.annotation.tailrec
import scala.collection.SortedSet
import scala.concurrent.duration._
import scala.math.BigDecimal.RoundingMode

trait PoALikeConsensus extends Consensus with ScorexLogging {
  def blockchain: BlockchainUpdater with NG
  def blockchainSettings: BlockchainSettings

  def roundTillSyncMillis: Long
  def roundAndSyncMillis: Long
  def banDurationBlocks: Int
  def warningsForBan: Int
  def maxBansPercentage: Int

  def time: Time

  import PoALikeConsensus._

  /**
    * Calculates current round's timestamps
    */
  def calculateRoundTimestamps(timestamp: Long): RoundTimestamps = {
    val genesisBlockTimestamp = blockchainSettings.custom.genesis.blockTimestamp
    val roundStart            = ((timestamp - genesisBlockTimestamp) / roundAndSyncMillis) * roundAndSyncMillis + genesisBlockTimestamp
    RoundTimestamps(roundStart, roundStart + roundTillSyncMillis, roundStart + roundAndSyncMillis - 1L)
  }

  /**
    * Calculates next round's expected timestamp given current round's timestamp
    */
  def nextRoundTimestamp(currentTimestamp: Long): Long = {
    val genesisBlockTimestamp = blockchainSettings.custom.genesis.blockTimestamp

    val blockchainLifeTime = currentTimestamp - genesisBlockTimestamp
    blockchainLifeTime - blockchainLifeTime % roundAndSyncMillis + roundAndSyncMillis + genesisBlockTimestamp
  }

  def currentMinerAndSkippedRounds(referenceTimestamp: Long, currentTime: Long, blockchainHeight: Int): Either[ValidationError, ConsensusRoundInfo] =
    for {
      skippedRounds <- countSkippedRounds(referenceTimestamp, currentTime)
      roundInfo <- determineMiner(currentTime, blockchainHeight, skippedRounds)
        .map(miner => ConsensusRoundInfo(miner, skippedRounds))
    } yield roundInfo

  protected def countSkippedRounds(referenceTimestamp: Long, currentBlockTimestamp: Long): Either[GenericError, Int] = {
    val expectedRoundStart  = nextRoundTimestamp(referenceTimestamp)
    val skippedRounds: Long = (currentBlockTimestamp - expectedRoundStart) / roundAndSyncMillis
    if (skippedRounds > Int.MaxValue) {
      Left(GenericError("Something wrong happened, skipped rounds counter has overflown integer's max value"))
    } else {
      Right(skippedRounds.toInt)
    }
  }

  override def getCurrentAndNextMiners(): Either[ValidationError, Seq[Address]] = {

    blockchain.lastBlock match {
      case Some(block) =>
        val currentTime = time.correctedTime()
        val height      = blockchain.height

        determineCurrentAndNextMiner(block.timestamp, currentTime, height)

      case None => Right(Seq())
    }
  }

  def determineMiner(timestamp: Long, currentHeight: Int, skipRounds: Int = 0): Either[ValidationError, Address] = {
    val validMiners       = blockchain.miners.currentMinersSet(timestamp)
    val minerToBanHistory = validMiners.map(address => address -> blockchain.minerBanHistory(address)).toMap

    determineMiner(validMiners, minerToBanHistory, currentHeight, skipRounds)
  }

  protected def determineMiner(
      validMiners: SortedSet[Address],
      minerToBanHistory: Map[Address, MinerBanHistory],
      currentHeight: Int,
      skipRounds: Int
  ): Either[ValidationError, Address] = {
    val minersCount = validMiners.size

    @tailrec
    def chooseMiner(it: Iterator[Address]): Either[ValidationError, Address] = {
      if (it.hasNext) {
        val nextMinerCandidate = it.next()
        val banHistory         = minerToBanHistory(nextMinerCandidate)
        if (banHistory.isNotBanned(currentHeight)) {
          Right(nextMinerCandidate)
        } else {
          chooseMiner(it)
        }
      } else {
        Left(GenericError("All miners are banned"))
      }
    }

    @tailrec
    def recursiveSearch(height: Int): Either[ValidationError, Address] = {
      blockchain
        .blockHeaderAt(height)
        .toRight[ValidationError](GenericError(s"Couldn't retrieve block at height $height from blockchain"))
        .map(_.signerData.generatorAddress) match {
        case Right(pastMiner) =>
          if (validMiners.contains(pastMiner)) {
            val nextMinerIterator = (validMiners.iteratorFrom(pastMiner) ++ validMiners.toIterator)
              .drop(1 + skipRounds % minersCount) //1 here is to drop miner for `currentHeight`
            chooseMiner(nextMinerIterator)
          } else if (height == 1) {
            val nextMinerIterator = validMiners.toIterator.drop((1 + skipRounds) % minersCount)
            if (nextMinerIterator.hasNext)
              Right(nextMinerIterator.next())
            else
              Left(GenericError("No miners in the network"))
          } else {
            recursiveSearch(height - 1)
          }

        case l @ Left(_) => l
      }
    }

    if (validMiners.isEmpty) {
      Left(GenericError(s"Couldn't determine miner: miner queue is empty!"))
    } else {
      recursiveSearch(currentHeight)
    }
  }

  private def determineCurrentAndNextMiner(referenceTimestamp: Long, currentTime: Long, currentHeight: Int): Either[ValidationError, Seq[Address]] = {
    for {
      skippedRounds <- countSkippedRounds(referenceTimestamp, currentTime)
      validMiners       = blockchain.miners.currentMinersSet(currentTime)
      minerToBanHistory = validMiners.map(address => address -> blockchain.minerBanHistory(address)).toMap

      currentAndNextMiners <- determineCurrentAndNextMiner(validMiners, minerToBanHistory, currentHeight, skippedRounds)
    } yield currentAndNextMiners
  }

  private def determineCurrentAndNextMiner(
      validMiners: SortedSet[Address],
      minerToBanHistory: Map[Address, MinerBanHistory],
      currentHeight: Int,
      skipRounds: Int
  ): Either[ValidationError, Seq[Address]] = {
    val minersCount = validMiners.size

    @tailrec
    def chooseMiner(it: Iterator[Address]): Either[ValidationError, Address] = {
      if (it.hasNext) {
        val nextMinerCandidate = it.next()
        val banHistory         = minerToBanHistory(nextMinerCandidate)
        if (banHistory.isNotBanned(currentHeight)) {
          Right(nextMinerCandidate)
        } else {
          chooseMiner(it)
        }
      } else {
        Left(GenericError("All miners are banned"))
      }
    }

    @tailrec
    def recursiveSearch(height: Int): Either[ValidationError, Seq[Address]] = {
      blockchain
        .blockHeaderAt(height)
        .toRight[ValidationError](GenericError(s"Couldn't retrieve block at height $height from blockchain"))
        .map(_.signerData.generatorAddress) match {
        case Right(pastMiner) =>
          if (validMiners.contains(pastMiner)) {
            val nextMinerIterator = (validMiners.iteratorFrom(pastMiner) ++ validMiners.toIterator)
              .drop(1 + skipRounds % minersCount) //1 here is to drop miner for `currentHeight`
            val curMinerEither = chooseMiner(nextMinerIterator).map(Seq(_))
            val nextMiner      = chooseMiner(nextMinerIterator).map(Seq(_)).getOrElse(Seq())

            curMinerEither.map(_ ++ nextMiner)
          } else if (height == 1) {
            val nextMinerIterator = validMiners.toIterator.drop((1 + skipRounds) % minersCount)
            if (nextMinerIterator.hasNext) {
              val curMiner = nextMinerIterator.next()
              Right(if (nextMinerIterator.hasNext) Seq(curMiner, nextMinerIterator.next()) else Seq(curMiner))
            } else
              Left(GenericError("No miners in the network"))
          } else {
            recursiveSearch(height - 1)
          }

        case Left(err) => Left(err)
      }
    }

    if (validMiners.isEmpty) {
      Left(GenericError(s"Couldn't determine miner: miner queue is empty!"))
    } else {
      recursiveSearch(currentHeight)
    }
  }

  def checkTxMicroblockTimestampBounds(microBlockTimestamp: Long, roundTimestamps: RoundTimestamps): Either[OutOfTimestampBounds, Long] = {
    val (leftBound, rightBound) = (roundTimestamps.roundStart, roundTimestamps.syncStart)

    Right[OutOfTimestampBounds, Long](microBlockTimestamp)
      .ensureOr(OutOfTimestampBounds.EarlierThanBound("TxMicroBlock", leftBound, _))(_ > leftBound)
      .ensureOr(OutOfTimestampBounds.OlderThanBound("TxMicroBlock", rightBound, _))(_ < rightBound)
  }

  /**
    * Performs PoA validation for given block
    * Returns post-action to be performed after block is appended
    */
  def blockConsensusValidation(currentTimestamp: => Long, block: Block): Either[ValidationError, ConsensusPostAction] = {
    val blockTime      = block.timestamp
    val parentUniqueId = block.reference

    for {
      parentHeight <- blockchain
        .heightOf(parentUniqueId)
        .toRight(GenericError(s"height: history does not contain parent $parentUniqueId"))

      parentHeader <- blockchain
        .parentHeader(block)
        .toRight(GenericError(s"History does not contain parent of '$block'"))

      _ <- validateBlockVersion(parentHeight, block, blockchainSettings.custom.functionality)
      currentTs = currentTimestamp
      _ <- Either.cond(blockTime - currentTs < MaxTimeDrift, (), BlockFromFuture(blockTime, currentTs))

      parentTimestamps @ RoundTimestamps(_, _, parentRoundEndTs)               = calculateRoundTimestamps(parentHeader.timestamp)
      blockTimestamps @ RoundTimestamps(roundStartTs, syncStartTs, roundEndTs) = calculateRoundTimestamps(blockTime)

      _ <- Either.cond(parentTimestamps != blockTimestamps, (), GenericError("Block is in the same round as its referenced block"))
      _ <- Right(blockTime)
        .ensure(s"not after parent's round end ($blockTime <= $parentRoundEndTs)")(_ > parentRoundEndTs)
        .ensure(s"before current round start ($blockTime < $roundStartTs)")(_ >= roundStartTs)
        .ensure(s"after current round end ($blockTime >= $roundStartTs)")(_ < roundEndTs)
        .ensure(s"after sync start ($blockTime >= $syncStartTs)")(_ < syncStartTs)
        .leftMap(descriptionStr => GenericError(s"Block's ${block.uniqueId} timestamp is $descriptionStr"))

      postAction <- calculatePostAction(block, parentHeader.timestamp, parentHeight)
      _ = log.trace(s"Calculated post action: $postAction")
    } yield postAction
  }.leftMap {
    case GenericError(x) => GenericError(s"Block $block is invalid: $x")
    case x               => x
  }

  /**
    * Calculates action, performed on state (blockchain) after the block gets successfully appended
    */
  def calculatePostAction(block: Block): Either[ValidationError, ConsensusPostAction] = {
    val parentUniqueId = block.reference
    for {
      parentHeight <- blockchain
        .heightOf(parentUniqueId)
        .toRight(GenericError(s"height: history does not contain parent ${block.reference}"))
      parentHeader <- blockchain
        .parentHeader(block)
        .toRight(GenericError(s"History does not contain parent of '$block'"))
      postAction <- calculatePostAction(block, parentHeader.timestamp, parentHeight)
      _ = log.trace(s"Calculated post action: $postAction")
    } yield postAction
  }

  /**
    * Shorter method to be used in [[blockConsensusValidation]]
    */
  protected def calculatePostAction(block: Block, parentTimestamp: Long, parentHeight: Int): Either[ValidationError, ConsensusPostAction] = {
    val minerQueue           = blockchain.miners
    val blockTime            = block.timestamp
    val validMiners          = minerQueue.currentMinersSet(blockTime)
    val minerToBanHistory    = validMiners.map(address => address -> blockchain.minerBanHistory(address)).toMap
    val candidateBlockHeight = parentHeight + 1
    for {
      skippedMinersInfo <- findSkippedMiners(validMiners, minerToBanHistory, blockTime, parentTimestamp, parentHeight)
      SkippedMinersInfo(skippedMiners, currentMinerOffset) = skippedMinersInfo

      expectedMiner <- determineMiner(validMiners, minerToBanHistory, parentHeight, currentMinerOffset)
      blockMiner = block.signerData.generatorAddress
      _ <- Either.cond(
        blockMiner == expectedMiner,
        (),
        GenericError(s"Block signer $blockMiner isn't a valid miner for height $candidateBlockHeight; Expected $expectedMiner") //есть такой кейс
      )
    } yield WarnFaultyMiners(blockMiner, validMiners, skippedMiners, candidateBlockHeight, blockTime, maxBansPercentage)
  }

  protected def findSkippedMiners(validMiners: SortedSet[Address],
                                  minerToBanHistory: Map[Address, MinerBanHistory],
                                  blockTimestamp: Long,
                                  parentBlockTimestamp: Long,
                                  parentHeight: Int): Either[ValidationError, SkippedMinersInfo] = {

    countSkippedRounds(parentBlockTimestamp, blockTimestamp).flatMap { skippedRounds =>
      if (parentHeight == 1 || maxBansPercentage == 0) { // for the first block after Genesis block
        Right(SkippedMinersInfo(List.empty, skippedRounds))
      } else {
        lazy val isPoaOptimisationActivated = blockchain.isFeatureActivated(BlockchainFeature.PoaOptimisationFix, parentHeight + 1)
        val minersCount                     = validMiners.size

        // to ban no more than maxBansPercentage in the worst case
        val roundsToCheck =
          if (skippedRounds > minersCount && isPoaOptimisationActivated) {
            val punishableLimit = minersCount * warningsForBan - (minersCount * (1 - maxBansPercentage / 100.0)).toInt
            skippedRounds.min(punishableLimit)
          } else {
            skippedRounds
          }

        val minerForRound = (0 until roundsToCheck).toList
          .traverse(determineMiner(validMiners, minerToBanHistory, parentHeight, _))

        minerForRound.map(skippedMiners => SkippedMinersInfo(skippedMiners, skippedRounds))
      }
    }
  }

  override def microBlockConsensusValidation(microBlock: MicroBlock): Either[ValidationError, Unit] = {
    for {
      lastBlock <- blockchain.lastBlock.toRight(GenericError(s"Last block not found"))
      roundTimestamps = calculateRoundTimestamps(lastBlock.timestamp)
      _ <- microBlock match {
        case _: TxMicroBlock => checkTxMicroblockTimestampBounds(microBlock.timestamp, roundTimestamps).void
        case _               => Right(())
      }
    } yield ()
  }
}

object PoALikeConsensus {

  private[consensus] val ScoreAdjustmentConstant = BigDecimal("18446744073709551616")

  def calculateBlockScore(skippedRounds: Long, blockTimestamp: Long): BigInt = {
    (ScoreAdjustmentConstant / (99 + skippedRounds) + BigDecimal(((blockTimestamp / 1E3) % 1.0E4) * (blockTimestamp / 1e10)))
      .setScale(0, RoundingMode.CEILING)
      .toBigInt()
  }

  sealed trait OutOfTimestampBounds extends ValidationError
  object OutOfTimestampBounds {
    case class EarlierThanBound(entityName: String, boundTimestamp: Long, currentTimestamp: Long) extends OutOfTimestampBounds {
      override def toString: String = s"Invalid $entityName timestamp '$currentTimestamp', it must be older than '$boundTimestamp'"
    }

    case class OlderThanBound(entityName: String, boundTimestamp: Long, currentTimestamp: Long) extends OutOfTimestampBounds {
      override def toString: String = s"Invalid $entityName timestamp '$currentTimestamp', it must be earlier than '$boundTimestamp'"
    }
  }

  case class ConsensusRoundInfo(expectedMiner: Address, skippedRounds: Long)
  case class RoundTimestamps(roundStart: Long, syncStart: Long, roundEnd: Long)
  case class SkippedMinersInfo(skippedMiners: List[Address], skippedRounds: Int)

  /**
    * ...............................................
    *                     .:-:.                     .  MaxDelay
    *                    :.   .:                    .
    *                  .:       :.                  .
    *                 .:         :.                 .
    *                .:           :                 .
    *               .:            .:                .
    *              .:              :.               .
    *              -.               -.              .
    *             -.                 -.             .
    *            .:                   :.            .
    *           .:                     :.           .
    *          .:                       :.          .
    *   .     .-                         :.     .   .
    *    :. .:                             :. .:    .
    *      *                                 *      . MinDelay
    * ...............................................
    * PreviousEvent                    TargetEvent
    */
  def calculatePeriodicDelay(currentTimestamp: Long,
                             targetTimestamp: Long,
                             fullInterval: Long,
                             alreadyChecked: Boolean = false,
                             minDelay: FiniteDuration = 100.millis,
                             maxDelay: FiniteDuration): FiniteDuration = {
    val position = 1.0 - (targetTimestamp - currentTimestamp).toDouble / fullInterval // in range [0; 1]

    if (alreadyChecked && position < 0.5) {
      maxDelay
    } else {
      def y(x: Double): Double = math.pow(math.sin(x * math.Pi), 2)
      ((maxDelay - minDelay).toMillis * y(position)).millis + minDelay
    }
  }

  def calcBansLimit(activeMiners: Int, maxBansPercentage: Int): Int = {
    if (maxBansPercentage == 0) {
      0
    } else {
      (activeMiners * maxBansPercentage) / 100
    }
  }
}
