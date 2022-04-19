package com.wavesenterprise.consensus

import cats.Show
import cats.implicits._
import com.wavesenterprise.consensus.MinerBanlistEntry.CancelledWarning
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.utils.TrimmedShowIterable

/**
  * Ban history for a miner.
  * Contains entries of two kinds: [[MinerBanlistEntry.Warning]] and [[MinerBanlistEntry.Ban]]
  * Logic:
  *   1. When a miner skips his mining round, he is given a Warning;
  *   2. If miner successfully mines a block in his round, Warnings are released;
  *   3. If miner gets a third Warning, three Warnings are substituted with a Ban entry;
  *   4. Banned miner is excluded from MinerQueue for [[banDurationBlocks]] blocks;
  */
trait MinerBanHistory {

  def warningsForBan: Int

  def unsavedDepth: Int

  def size: Int

  def entries: Seq[MinerBanlistEntry]

  def isNotBanned(atHeight: Int): Boolean = !isBanned(atHeight)

  def isBanned(atHeight: Int): Boolean

  def hasWarnings: Boolean

  def warnOrBan(atHeight: Int, currentTimestamp: Long): MinerBanHistory

  def dropWarnings(blockTimestamp: Long): (Seq[CancelledWarning], MinerBanHistory)

  def rollback(targetTimestamp: Long, cancelledWarnings: Seq[CancelledWarning], targetOrdering: Ordering[MinerBanlistEntry]): MinerBanHistory
}

object MinerBanHistory {

  val legacyBanlistEntryOrdering: Ordering[MinerBanlistEntry] = Ordering.fromLessThan((e1, e2) => e1.timestamp >= e2.timestamp)
  val modernBanlistEntryOrdering: Ordering[MinerBanlistEntry] = Ordering[Long].on[MinerBanlistEntry](_.timestamp).reverse

  def selectOrdering(activatedFeatures: Map[Short, Int], height: Int): Ordering[MinerBanlistEntry] = {
    if (activatedFeatures.get(BlockchainFeature.PoaOptimisationFix.id).exists(_ <= height)) {
      modernBanlistEntryOrdering
    } else {
      legacyBanlistEntryOrdering
    }
  }

  implicit val toPrintable: Show[MinerBanHistory] = { history =>
    history.entries.mkStringTrimmed(trim = history.warningsForBan + 2)
  }
}

trait MinerBanHistoryBuilder {
  def empty: MinerBanHistory

  /**
    * @param maybeKnownSize – If the size of the collection is known, then it is better to use it,
    *                         otherwise, the base size method will be used, which can evaluate the entire sequence.
    * @param newEntriesCount – The number of entries that have not yet been saved in persistent storage.
    */
  def build(entries: Seq[MinerBanlistEntry], maybeKnownSize: Option[Int] = None, newEntriesCount: Int = 0): MinerBanHistory
}
