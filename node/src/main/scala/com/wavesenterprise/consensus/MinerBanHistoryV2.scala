package com.wavesenterprise.consensus

import cats.implicits._
import com.wavesenterprise.consensus.MinerBanlistEntry.{Ban, PriorWarningsInfo, Warning, _}
import com.wavesenterprise.utils.ScorexLogging

final case class MinerBanHistoryV2 private (
    banHistory: Stream[MinerBanlistEntry],
    banDurationBlocks: Int,
    warningsForBan: Int,
    /** Stored as field so it is not necessary to evaluate the entire lazy list.
      * This approach requires caution to ensure consistency. */
    size: Int,
    /** The number of entries to be updated in persistent storage */
    unsavedDepth: Int
) extends ScorexLogging
    with MinerBanHistory {

  override def entries: Seq[MinerBanlistEntry] = banHistory

  override def isBanned(atHeight: Int): Boolean =
    banHistory
      .takeWhile {
        case _: Warning => true
        case ban: Ban   => ban.endHeight(banDurationBlocks) >= atHeight
      }
      .exists {
        case ban: Ban => atHeight >= ban.beginHeight && atHeight < ban.endHeight(banDurationBlocks)
        case _        => false
      }

  override def hasWarnings: Boolean =
    banHistory.takeWhile(_.isInstanceOf[Warning]).nonEmpty

  override def warnOrBan(atHeight: Int, currentTimestamp: Long): MinerBanHistory = {
    if (banHistory.headOption.exists(_.timestamp >= currentTimestamp)) {
      this
    } else {
      val lastWarnings = banHistory.takeWhile(_.isInstanceOf[Warning]).toArray
      val (newHistory, newSize, newUnsavedDepth) =
        if (lastWarnings.length == warningsForBan - 1) {
          val newBan                 = Ban(currentTimestamp, atHeight, PriorWarningsInfo(lastWarnings.map(_.timestamp)))
          val discardedWarningsCount = warningsForBan - 1
          val updatedHistory         = newBan #:: banHistory.drop(discardedWarningsCount)
          val updatedSize            = size - discardedWarningsCount + 1
          val updatedUnsavedDepth    = math.max(0, unsavedDepth - discardedWarningsCount) + 1
          (updatedHistory, updatedSize, updatedUnsavedDepth)
        } else {
          val updatedHistory = Warning(currentTimestamp) #:: banHistory
          (updatedHistory, size + 1, unsavedDepth + 1)
        }

      MinerBanHistoryV2(newHistory, banDurationBlocks, warningsForBan, newSize, newUnsavedDepth)
    }
  }

  override def dropWarnings(blockTimestamp: Long): (Seq[CancelledWarning], MinerBanHistory) = {
    val (edgeWarnings, updatedHistoryEntries) = banHistory.span(_.isInstanceOf[Warning])

    val warnings        = edgeWarnings.collect { case w: Warning => w }.toVector
    val warningsCount   = warnings.size
    val newSize         = size - warningsCount
    val newUnsavedDepth = math.max(0, unsavedDepth - warningsCount)
    val updatedHistory  = MinerBanHistoryV2(updatedHistoryEntries, banDurationBlocks, warningsForBan, newSize, newUnsavedDepth)

    val cancelledWarnings = warnings.map(CancelledWarning(blockTimestamp, _))

    cancelledWarnings -> updatedHistory
  }

  override def rollback(targetTimestamp: Long,
                        cancelledWarnings: Seq[CancelledWarning],
                        targetOrdering: Ordering[MinerBanlistEntry]): MinerBanHistory = {
    log.trace(s"""Performing MinerBanHistory rollback to targetTimestamp '$targetTimestamp'
                 |old MinerBanHistory: [${banHistory.show}]
                 |cancelledWarnings: [${cancelledWarnings.mkString(", ")}]""".stripMargin)

    val (expiredEntries, restEntries) = banHistory.span(_.timestamp > targetTimestamp)
    if (expiredEntries.nonEmpty) {
      log.trace(s"MinerBanHistory.rollback expired entries: [${expiredEntries.mkString(", ")}]")
    }

    val firstExpiredBanWarnings = expiredEntries.reverse
      .collectFirst({ case ban: Ban => ban })
      .toSeq
      .flatMap(_.priorWarningTimestamps.toWarnings)

    val filteredCancelledWarnings = cancelledWarnings.collect {
      case CancelledWarning(cancelTs, warning) if cancelTs > targetTimestamp => warning
    }

    val warningsToAdd = (firstExpiredBanWarnings ++ filteredCancelledWarnings)
      .filter(_.timestamp <= targetTimestamp)
      .sorted(targetOrdering)

    if (warningsToAdd.nonEmpty) {
      log.trace(s"MinerBanHistory.rollback warnings to add: [${warningsToAdd.mkString(", ")}]")
    }

    val newUnsavedDepth = math.max(0, unsavedDepth - expiredEntries.size) + warningsToAdd.size
    val newSize         = size - expiredEntries.size + warningsToAdd.size
    val updatedEntries  = warningsToAdd.toStream #::: restEntries
    val newHistory      = MinerBanHistoryV2(updatedEntries, banDurationBlocks, warningsForBan, newSize, newUnsavedDepth)
    log.trace(s"MinerBanHistory after rollback: [${newHistory.banHistory.show}]")
    newHistory
  }
}

class MinerBanHistoryBuilderV2(banDuration: Int, warningsForBan: Int) extends MinerBanHistoryBuilder {
  val empty: MinerBanHistory = MinerBanHistoryV2(Stream.empty, banDuration, warningsForBan, 0, 0)

  def build(entries: Seq[MinerBanlistEntry], maybeKnownSize: Option[Int], newEntriesCount: Int): MinerBanHistory = {
    MinerBanHistoryV2(
      banHistory = entries.toStream,
      banDurationBlocks = banDuration,
      warningsForBan = warningsForBan,
      size = maybeKnownSize.getOrElse(entries.size),
      unsavedDepth = newEntriesCount
    )
  }
}
