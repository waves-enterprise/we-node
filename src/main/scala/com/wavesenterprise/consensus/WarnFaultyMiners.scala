package com.wavesenterprise.consensus

import cats.Show
import cats.implicits._
import com.wavesenterprise.account.Address
import com.wavesenterprise.consensus.MinerBanlistEntry.CancelledWarning
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.features.FeatureProvider.FeatureProviderExt
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.utils.ScorexLogging

import scala.annotation.tailrec
import scala.collection.SortedSet

case class WarnFaultyMiners(currentMiner: Address,
                            currentMinersSet: SortedSet[Address],
                            faultyPastMiners: List[Address],
                            currentHeight: Int,
                            blockTimestamp: Long,
                            maxBansPercentage: Int)
    extends ConsensusPostAction
    with ScorexLogging {
  import WarnFaultyMiners._

  def apply(blockchain: Blockchain): ConsensusPostActionDiff = {
    log.trace("Performing ConsensusPostAction")
    val minerBanHistory = blockchain.minerBanHistory(currentMiner)
    log.trace(s"MinerBanHistory for $currentMiner: ${minerBanHistory.show}")

    val (cancelledWarnings, initialBanHistoryDiff) = if (minerBanHistory.hasWarnings) {
      log.trace(s"Warnings have been dropped from MinerBanHistory of $currentMiner")
      val (deletedWarnings, updatedHistory) = minerBanHistory.dropWarnings(blockTimestamp)
      Map(currentMiner -> deletedWarnings) -> Map(currentMiner -> updatedHistory)
    } else {
      Map.empty[Address, Seq[CancelledWarning]] -> Map.empty[Address, MinerBanHistory]
    }

    val minerToBanHistory = currentMinersSet.map(address => address -> blockchain.minerBanHistory(address)).toMap
    val withHistoryFix    = blockchain.isFeatureActivated(BlockchainFeature.PoaOptimisationFix, blockchain.height)
    val MinersPunishmentResult(banHistoryDiff, punishmentReport) =
      punishMiners(faultyPastMiners.toIterator, minerToBanHistory, initialBanHistoryDiff, withHistoryFix)
    if (punishmentReport.nonEmpty) {
      log.trace(s"""Punished miners:
             |${punishmentReport.show}""".stripMargin)
    }

    ConsensusPostActionDiff(banHistoryDiff, cancelledWarnings)
  }

  private def punishMiners(faultyMiners: Iterator[Address],
                           minerToBanHistory: Map[Address, MinerBanHistory],
                           minersBanHistoryDiff: Map[Address, MinerBanHistory],
                           withHistoryFix: Boolean): MinersPunishmentResult = {
    val activeMiners: Int      = currentMinersSet.size
    val bannedMiners           = currentMinersSet.filter(mAddress => minerToBanHistory(mAddress).isBanned(currentHeight))
    val bannedMinersCount: Int = bannedMiners.size
    val bansLimit: Int         = PoALikeConsensus.calcBansLimit(activeMiners, maxBansPercentage)
    log.trace(s"activeMiners: $activeMiners, bannedMiners: $bannedMinersCount, bansLimit: $bansLimit")

    if (bannedMinersCount > 0) {
      log.trace(s"Current banned miners: ${bannedMiners.mkString("[", ",", "]")}")
    }

    @tailrec
    def banRecursively(bannedMinersCount: Int,
                       minersBanHistoryDiff: Map[Address, MinerBanHistory],
                       punishmentReport: MinersPunishmentReport = MinersPunishmentReport.empty): MinersPunishmentResult = {
      if (faultyMiners.hasNext && isBanPossible(bannedMinersCount, bansLimit)) {
        val faultyMiner = faultyMiners.next()
        val oldHistory = {
          if (withHistoryFix)
            minersBanHistoryDiff.getOrElse(faultyMiner, minerToBanHistory(faultyMiner))
          else
            minerToBanHistory(faultyMiner)
        }
        val updatedBanHistory = oldHistory.warnOrBan(currentHeight, blockTimestamp)
        log.trace(s"""MinerBanHistory for '$faultyMiner':
                       |Old: ${oldHistory.show}
                       |Updated: ${updatedBanHistory.show}""".stripMargin)
        val updatedBanHistoryDiff = minersBanHistoryDiff.updated(faultyMiner, updatedBanHistory)

        if (updatedBanHistory.isBanned(currentHeight)) {
          banRecursively(bannedMinersCount + 1, updatedBanHistoryDiff, punishmentReport.copy(banned = faultyMiner +: punishmentReport.banned))
        } else {
          banRecursively(bannedMinersCount, updatedBanHistoryDiff, punishmentReport.copy(warned = faultyMiner +: punishmentReport.warned))
        }
      } else {
        MinersPunishmentResult(minersBanHistoryDiff, punishmentReport)
      }
    }

    banRecursively(bannedMinersCount, minersBanHistoryDiff)
  }

  private def isBanPossible(bannedMiners: Int, bansLimit: Int): Boolean = {
    bannedMiners < bansLimit
  }

  override val toString: String = {
    s"""WarnFaultyMiners(
         |currentMiner: $currentMiner,
         |faultyPastMiners: $faultyPastMiners,
         |currentHeight: $currentHeight)""".stripMargin
  }
}

object WarnFaultyMiners {

  case class MinersPunishmentReport(warned: List[Address], banned: List[Address]) {
    val nonEmpty: Boolean = warned.nonEmpty || banned.nonEmpty
  }

  object MinersPunishmentReport {
    def empty = MinersPunishmentReport(List.empty, List.empty)

    implicit val toPrintable: Show[MinersPunishmentReport] = { report =>
      s"""warned: [${report.warned.mkString(", ")}]
           |banned: [${report.banned.mkString(", ")}]""".stripMargin
    }
  }

  case class MinersPunishmentResult(banHistoryDiff: Map[Address, MinerBanHistory], report: MinersPunishmentReport)
}
