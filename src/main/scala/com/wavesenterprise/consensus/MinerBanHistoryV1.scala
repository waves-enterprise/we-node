package com.wavesenterprise.consensus

import cats.implicits._
import com.wavesenterprise.consensus.MinerBanlistEntry.{Ban, PriorWarningsInfo, Warning, _}
import com.wavesenterprise.utils.ScorexLogging

import scala.collection.immutable.SortedSet

final case class MinerBanHistoryV1 private (banHistory: SortedSet[MinerBanlistEntry], banDurationBlocks: Int, warningsForBan: Int)
    extends ScorexLogging
    with MinerBanHistory {

  override def entries: Seq[MinerBanlistEntry] = banHistory.toSeq

  override def isBanned(atHeight: Int): Boolean =
    banHistory.exists {
      case ban: Ban => atHeight >= ban.beginHeight && atHeight < ban.endHeight(banDurationBlocks)
      case _        => false
    }

  override def hasWarnings: Boolean =
    banHistory.collectFirst({ case _: Warning => () }).isDefined

  override def warnOrBan(atHeight: Int, currentTimestamp: Long): MinerBanHistoryV1 = {
    val lastWarnings = banHistory.takeWhile(_.isInstanceOf[Warning]).toArray
    val updatedHistory =
      if (lastWarnings.length == warningsForBan - 1) {
        banHistory.drop(warningsForBan - 1) + Ban(currentTimestamp, atHeight, PriorWarningsInfo(lastWarnings.map(_.timestamp)))
      } else {
        banHistory + Warning(currentTimestamp)
      }

    MinerBanHistoryV1(updatedHistory, banDurationBlocks, warningsForBan)
  }

  override def dropWarnings(blockTimestamp: Long): (Seq[CancelledWarning], MinerBanHistoryV1) = {
    val (warnings, updatedHistoryEntries) = banHistory.toList.map {
      case w: Warning => Left(w)
      case b: Ban     => Right(b)
    }.separate

    val updatedHistory    = MinerBanHistoryV1(updatedHistoryEntries, banDurationBlocks, warningsForBan)(banHistory.ordering)
    val cancelledWarnings = warnings.map(CancelledWarning(blockTimestamp, _))

    cancelledWarnings -> updatedHistory
  }

  override def rollback(targetTimestamp: Long,
                        cancelledWarnings: Seq[CancelledWarning],
                        targetOrdering: Ordering[MinerBanlistEntry]): MinerBanHistoryV1 = {
    log.trace(s"""Performing MinerBanHistory rollback to targetTimestamp '$targetTimestamp'
                 |old MinerBanHistory: [${banHistory.show}]
                 |cancelledWarnings: [${cancelledWarnings.mkString(", ")}]""".stripMargin)

    val (expiredEntries, restEntries) = banHistory.partition(_.timestamp > targetTimestamp)
    if (expiredEntries.nonEmpty) {
      log.trace(s"MinerBanHistory.rollback expired entries: [${expiredEntries.mkString(", ")}]")
    }

    val firstExpiredBanWarnings = expiredEntries.toSeq.reverse
      .collectFirst({ case ban: Ban => ban })
      .toSeq
      .flatMap(_.priorWarningTimestamps.toWarnings)

    val filteredCancelledWarnings = cancelledWarnings.collect {
      case CancelledWarning(cancelTs, warning) if cancelTs > targetTimestamp => warning
    }

    val warningsToAdd: Seq[MinerBanlistEntry] = (firstExpiredBanWarnings ++ filteredCancelledWarnings).filter(_.timestamp <= targetTimestamp)

    if (warningsToAdd.nonEmpty) {
      log.trace(s"MinerBanHistory.rollback warnings to add: [${warningsToAdd.mkString(", ")}]")
    }

    val warningsOrdered = SortedSet(warningsToAdd: _*)(targetOrdering)

    val newHistory = MinerBanHistoryV1(warningsOrdered | restEntries, banDurationBlocks, warningsForBan)
    log.trace(s"MinerBanHistory after rollback: [${newHistory.banHistory.show}]")
    newHistory
  }

  override def unsavedDepth: Int = banHistory.size

  override def size: Int = banHistory.size

  def combine(that: MinerBanHistoryV1): MinerBanHistoryV1 = {
    MinerBanHistoryV1(banHistory | that.banHistory, banDurationBlocks, warningsForBan)
  }
}

object MinerBanHistoryV1 {
  def apply(
      banHistory: Seq[MinerBanlistEntry],
      banDuration: Int,
      warningsForBan: Int
  )(implicit ordering: Ordering[MinerBanlistEntry]): MinerBanHistoryV1 = {
    MinerBanHistoryV1(SortedSet(banHistory: _*), banDuration, warningsForBan)
  }
}

class MinerBanHistoryBuilderV1(banDuration: Int, warningsForBan: Int)(implicit ordering: Ordering[MinerBanlistEntry]) extends MinerBanHistoryBuilder {
  val empty: MinerBanHistory = MinerBanHistoryV1(Seq.empty, banDuration, warningsForBan)

  def build(entries: Seq[MinerBanlistEntry], maybeKnownSize: Option[Int], newEntriesCount: Int): MinerBanHistory = {
    MinerBanHistoryV1(entries, banDuration, warningsForBan)
  }
}
