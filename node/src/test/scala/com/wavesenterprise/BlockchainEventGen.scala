package com.wavesenterprise

import com.wavesenterprise.state.{AppendedBlockHistory, BlockAppended, EventResult, MicroBlockAppended, RollbackCompleted}
import org.scalacheck.Gen
import org.scalatest.Suite

import scala.collection.JavaConverters._

trait BlockchainEventGen extends BlockGen { _: Suite =>

  def microBlockAppendedGen(transactionsCount: Int = 5): Gen[MicroBlockAppended] =
    for {
      randomTxs <- randomTransactionsGen(transactionsCount)
    } yield MicroBlockAppended(randomTxs)

  def blockAppendedGen(transactionsCount: Int = 10, height: Int = 1): Gen[BlockAppended] =
    for {
      signer    <- accountGen
      randomTxs <- randomTransactionsGen(transactionsCount)
      block     <- blockGen(randomTxs, signer)
    } yield
      BlockAppended(
        block.uniqueId,
        block.reference,
        randomTxs,
        block.signerData.generator.toAddress.bytes,
        height,
        block.version,
        block.timestamp,
        block.blockFee(),
        block.bytes().length,
        block.featureVotes
      )

  def rollbackCompletedGen(transactionsCount: Int = 5): Gen[RollbackCompleted] =
    for {
      returnToBlock <- randomSignerBlockGen
      randomTxs     <- randomTransactionsGen(transactionsCount)
    } yield RollbackCompleted(returnToBlock.uniqueId, randomTxs)

  def appendedBlockHistoryGen(transactionsCount: Int = 10, height: Int = 1): Gen[AppendedBlockHistory] =
    for {
      signer    <- accountGen
      randomTxs <- randomTransactionsGen(transactionsCount)
      block     <- blockGen(randomTxs, signer)
    } yield
      AppendedBlockHistory(
        block.uniqueId,
        block.reference,
        randomTxs,
        block.signerData.generator.toAddress.bytes,
        height,
        block.version,
        block.timestamp,
        block.blockFee(),
        block.bytes().length,
        block.featureVotes
      )

  def appendedBlockHistoryEventsGen(maxEventCount: Int, transactionsCount: Int = 10, startHeight: Int = 1): Gen[List[AppendedBlockHistory]] =
    (for {
      eventsCount <- Gen.chooseNum(1, maxEventCount)
      events <- Gen.sequence {
        (startHeight until startHeight + eventsCount).map { height =>
          appendedBlockHistoryGen(transactionsCount, height)
        }
      }
    } yield events).map(_.asScala.toList)

  def randomEventResultsGen(eventsCount: Int, startHeight: Int = 1, includeRollback: Boolean = false): Gen[List[EventResult]] =
    Gen
      .sequence {
        (startHeight until startHeight + eventsCount).map { height =>
          randomEventResultGen(height)
        }
      }
      .map(_.asScala.toList)

  def randomEventResultGen(height: Int = 1): Gen[EventResult] =
    for {
      transactionsCount <- Gen.chooseNum(1, 10)
      randomEvent <- Gen.oneOf(
        microBlockAppendedGen(transactionsCount),
        blockAppendedGen(transactionsCount, height)
      )
    } yield randomEvent
}
