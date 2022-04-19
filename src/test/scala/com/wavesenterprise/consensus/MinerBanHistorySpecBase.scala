package com.wavesenterprise.consensus

import com.wavesenterprise.consensus.MinerBanlistEntry._
import com.wavesenterprise.database._
import com.wavesenterprise.{NoShrink, TransactionGen}
import com.google.common.io.ByteStreams.newDataInput
import org.scalacheck.Gen
import org.scalatest.{Assertion, FreeSpec, Matchers}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

abstract class MinerBanHistorySpecBase extends FreeSpec with Matchers with TransactionGen with ScalaCheckPropertyChecks with NoShrink {

  val banDurationBlocks = 100
  val warningsForBan    = 3

  implicit val minerBanHistoryOrdering = MinerBanHistory.modernBanlistEntryOrdering

  def minerBanHistoryBuilder(
      banDurationBlocks: Int = banDurationBlocks,
      warningsForBan: Int = warningsForBan
  ): MinerBanHistoryBuilder

  val genBanHistory: Gen[MinerBanHistory] = for {
    warningCount       <- Gen.chooseNum(1, 1000)
    referenceTimestamp <- ntpTimestampGen
    banHistory = (1 to warningCount)
      .foldLeft(minerBanHistoryBuilder().empty) {
        case (mbh, i) =>
          mbh.warnOrBan(i, referenceTimestamp + i)
      }
  } yield banHistory

  "Database serialization round trip" in {
    forAll(genBanHistory) { minerBanHistory =>
      val entriesSeq    = minerBanHistory.entries
      val bytesSeq      = entriesSeq.map(_.bytes)
      val parsedEntries = bytesSeq.map(bytes => MinerBanlistEntry.fromDataInput(newDataInput(bytes)).right.get)
      parsedEntries should contain theSameElementsAs entriesSeq
    }
  }

  "Database serialization round trip (using database functions)" in {
    forAll(genBanHistory) { minerBanHistory =>
      val entriesSeq    = minerBanHistory.entries
      val bytes         = writeMinerBanlistEntries(entriesSeq)
      val parsedEntries = readMinerBanlistEntries(bytes)
      parsedEntries should contain theSameElementsAs entriesSeq
    }
  }

  "Single warning doesn't result in a ban" in {
    forAll(Gen.chooseNum(10, 1000)) { maybeBanHeight =>
      val mbh = minerBanHistoryBuilder().build(Seq.empty).warnOrBan(maybeBanHeight, 123L)
      historyInvariants(mbh)

      (1 until maybeBanHeight + banDurationBlocks)
        .foreach(height => assert(mbh.isNotBanned(height), s"Failed at $height"))
    }
  }
  "Double warning doesn't result in a ban" in {
    forAll(Gen.chooseNum(10, 1000)) { maybeBanHeight =>
      val mbh = minerBanHistoryBuilder()
        .build(Seq.empty)
        .warnOrBan(maybeBanHeight, 123L)
        .warnOrBan(maybeBanHeight + 1, 124L)

      historyInvariants(mbh)

      (1 until maybeBanHeight + banDurationBlocks + 1)
        .foreach(height => assert(mbh.isNotBanned(height), s"Failed at $height"))
    }
  }
  "Third warning results in a ban" in {
    forAll(Gen.chooseNum(10, 1000)) { banHeight =>
      val banHistory =
        Seq(Warning(1230L), Warning(1231L))

      val mbh = minerBanHistoryBuilder().build(banHistory).warnOrBan(banHeight, 1232L)

      historyInvariants(mbh)

      (1 until banHeight)
        .foreach(height => assert(mbh.isNotBanned(height), s"Failed at $height"))

      (banHeight until banHeight + banDurationBlocks)
        .foreach(height => assert(mbh.isBanned(height), s"Failed at $height"))
    }
  }
  "Second ban adds just fine" in {
    forAll(Gen.chooseNum(10, 500), Gen.chooseNum(601, 1000)) { (firstBanHeight, secondBanHeight) =>
      val banHistory = Seq(Warning(1236L), Warning(1235L), Ban(1234L, firstBanHeight, PriorWarningsInfo(Array(123L, 234L))))

      val mbh = minerBanHistoryBuilder().build(banHistory).warnOrBan(secondBanHeight, 1237L)

      historyInvariants(mbh)

      (1 until firstBanHeight)
        .foreach(height => assert(mbh.isNotBanned(height), s"Failed at $height: shouldn't be banned there"))

      (firstBanHeight until firstBanHeight + banDurationBlocks)
        .foreach(height => assert(mbh.isBanned(height), s"Failed at $height: should be banned there"))

      val postFirstBanHeight = firstBanHeight + banDurationBlocks + 1

      (postFirstBanHeight until secondBanHeight)
        .foreach(height => assert(mbh.isNotBanned(height), s"Failed at $height: shouldn't be banned there"))

      (secondBanHeight until secondBanHeight + banDurationBlocks)
        .foreach(height => assert(mbh.isBanned(height), s"Failed at $height: should be banned there"))

      val postSecondBanHeight = secondBanHeight + banDurationBlocks + 1

      (postSecondBanHeight until postSecondBanHeight + 100)
        .foreach(height => assert(mbh.isNotBanned(height), s"Failed at $height: shouldn't be banned there"))

    }
  }
  "Should release warnings" in {
    forAll(Gen.chooseNum(10, 1000)) { banHeight =>
      val banHistory = Seq(Warning(1231L), Warning(1230L))

      val (cancelledWarnings, updatedHistory) = minerBanHistoryBuilder()
        .build(banHistory)
        .dropWarnings(1232L)

      historyInvariants(updatedHistory)

      val resultHistory = updatedHistory.warnOrBan(banHeight, 1233L)

      historyInvariants(resultHistory)

      cancelledWarnings.map(_.warning) shouldBe banHistory
      assert(resultHistory.isNotBanned(banHeight), s"Shouldn't be banned, because two warnings had to be dropped")
    }
  }
  "Should release warnings and leave the old bans in place" in {
    forAll(Gen.chooseNum(10, 1000), Gen.chooseNum(1500, 3000)) { (oldBanHeight, newBanHeight) =>
      val banHistory = Seq(Warning(1231L), Warning(1230L), Ban(123L, oldBanHeight, PriorWarningsInfo(Array(1L, 2L))))

      val (cancelledWarnings, updatedHistory) = minerBanHistoryBuilder()
        .build(banHistory)
        .dropWarnings(12321L)

      historyInvariants(updatedHistory)

      val resultHistory = updatedHistory.warnOrBan(newBanHeight, 12322L)

      historyInvariants(resultHistory)

      cancelledWarnings.map(_.warning) shouldBe banHistory.filter(_.isInstanceOf[Warning])
      assert(resultHistory.isBanned(oldBanHeight), "Should be banned at old ban height")
      assert(resultHistory.isNotBanned(newBanHeight), "Shouldn't be banned, because two warnings had to be dropped")
    }
  }

  "After the ban the miner is okay" in {
    forAll(Gen.choose(10, 1000)) { banHeight =>
      val banHistory = Seq(Ban(123L, banHeight, PriorWarningsInfo(Array(1L, 2L))))

      val unbanHeight = banHeight + banDurationBlocks + 1
      val mbh         = minerBanHistoryBuilder().build(banHistory).warnOrBan(unbanHeight + 15, 1232L)

      historyInvariants(mbh)

      (unbanHeight to banHeight + 100)
        .foreach(height => assert(mbh.isNotBanned(height), s"Failed at $height"))
    }
  }

  "rollback" - {
    "case when nothing to rollback" in {
      val historyEntries = Seq(Warning(1231L), Warning(1230L), Ban(123L, 10, PriorWarningsInfo(Array(1L, 2L))))

      val banHistory = minerBanHistoryBuilder().build(historyEntries)

      historyInvariants(banHistory)

      val resultHistory = banHistory
        .rollback(Long.MaxValue, Seq.empty, MinerBanHistory.modernBanlistEntryOrdering)

      historyInvariants(resultHistory)

      resultHistory.entries should contain theSameElementsAs historyEntries
    }

    "rollback warnings" in {
      val oldBan         = Ban(123L, 10, PriorWarningsInfo(Array(1L, 2L)))
      val historyEntries = Seq(Warning(1231L), Warning(1230L), oldBan)

      val banHistory = minerBanHistoryBuilder().build(historyEntries)
      historyInvariants(banHistory)
      val rolledHistory = banHistory.rollback(1200L, Seq.empty, MinerBanHistory.modernBanlistEntryOrdering)
      historyInvariants(rolledHistory)

      rolledHistory.hasWarnings shouldBe false
      rolledHistory.entries.headOption shouldBe Some(oldBan)
    }

    "rollback warnings (when exists cancelled warnings)" in {
      val oldBan         = Ban(123L, 10, PriorWarningsInfo(Array(1L, 2L)))
      val historyEntries = Seq(Warning(1231L), Warning(1230L), oldBan)

      val cancelledWarnings = Seq(CancelledWarning(1201L, Warning(1131L)), CancelledWarning(1201L, Warning(1130L)))

      val banHistory = minerBanHistoryBuilder().build(historyEntries)
      historyInvariants(banHistory)
      val rolledHistory = banHistory.rollback(1200L, cancelledWarnings, MinerBanHistory.modernBanlistEntryOrdering)
      historyInvariants(rolledHistory)

      rolledHistory.entries shouldBe Seq(Warning(1131L), Warning(1130L), oldBan)
    }

    "rolled-back ban turns into two warnings (when their timestamps are in past)" in {
      val banHistory = minerBanHistoryBuilder().build(Seq(Ban(100L, 10, PriorWarningsInfo(Array(50L, 70L)))))
      historyInvariants(banHistory)
      val rolledBack = banHistory.rollback(85, Seq.empty, MinerBanHistory.modernBanlistEntryOrdering)
      historyInvariants(rolledBack)

      rolledBack.hasWarnings shouldBe true
      rolledBack.isBanned(11) shouldBe false
      rolledBack.entries should contain theSameElementsAs Seq(Warning(50L), Warning(70L))
    }

    "rolled-back ban turns into two warnings (when their timestamps are in past and exists cancelled warnings)" in {
      val banHistory = minerBanHistoryBuilder().build(Seq(Ban(100L, 10, PriorWarningsInfo(Array(50L, 70L)))))
      historyInvariants(banHistory)
      val cancelledWarnings = Seq(CancelledWarning(86L, Warning(72L)), CancelledWarning(86L, Warning(71L)))

      val rolledBack = banHistory.rollback(85, cancelledWarnings, MinerBanHistory.modernBanlistEntryOrdering)
      historyInvariants(rolledBack)

      rolledBack.hasWarnings shouldBe true
      rolledBack.isBanned(11) shouldBe false
      rolledBack.entries shouldBe Seq(Warning(72L), Warning(71L), Warning(70L), Warning(50L))
    }

    "rolled-back ban doesn't leave warnings if their timestamps are in future" in {
      val banHistory = minerBanHistoryBuilder().build(Seq(Ban(100L, 10, PriorWarningsInfo(Array(50L, 70L)))))
      historyInvariants(banHistory)
      val rolledBack = banHistory.rollback(40, Seq.empty, MinerBanHistory.modernBanlistEntryOrdering)
      historyInvariants(rolledBack)

      rolledBack.hasWarnings shouldBe false
      rolledBack.isBanned(11) shouldBe false
      rolledBack.entries shouldBe Seq.empty[MinerBanlistEntry]
    }

    "rolled-back ban doesn't leave warnings if their timestamps are in future (when exists cancelled warnings)" in {
      val banHistory = minerBanHistoryBuilder().build(Seq(Ban(100L, 10, PriorWarningsInfo(Array(50L, 70L)))))
      historyInvariants(banHistory)
      val cancelledWarnings = Seq(CancelledWarning(73L, Warning(72L)), CancelledWarning(73L, Warning(71L)))
      val rolledBack        = banHistory.rollback(40, cancelledWarnings, MinerBanHistory.modernBanlistEntryOrdering)
      historyInvariants(rolledBack)

      rolledBack.hasWarnings shouldBe false
      rolledBack.isBanned(11) shouldBe false
      rolledBack.entries shouldBe Seq.empty[MinerBanlistEntry]
    }

    "rollback multiple bans, leave warnings of the last one" in {
      val banHistory = minerBanHistoryBuilder().build(
        Seq(
          Ban(400L, 40, PriorWarningsInfo(Array(350L, 370L))),
          Ban(300L, 30, PriorWarningsInfo(Array(250L, 270L))),
          Ban(200L, 20, PriorWarningsInfo(Array(150L, 170L))),
          Ban(100L, 10, PriorWarningsInfo(Array(50L, 70L)))
        ))

      historyInvariants(banHistory)
      val rolledBack = banHistory.rollback(190L, Seq.empty, MinerBanHistory.modernBanlistEntryOrdering)
      historyInvariants(rolledBack)

      rolledBack.hasWarnings shouldBe true
      rolledBack.isBanned(11) shouldBe true
      rolledBack.entries shouldBe Seq(Warning(170L), Warning(150L), Ban(100L, 10, PriorWarningsInfo(Array(50L, 70L))))
    }

    "rollback one of two warnings" in {
      val banHistory = minerBanHistoryBuilder().build(Seq(Warning(2000L), Warning(1000L)))
      historyInvariants(banHistory)
      val rolledBack = banHistory.rollback(1000L, Seq.empty, MinerBanHistory.modernBanlistEntryOrdering)
      historyInvariants(rolledBack)

      rolledBack.hasWarnings shouldBe true
      rolledBack.entries shouldBe Seq(Warning(1000L))
    }

    "rollback all cancelled warnings" in {
      val banHistory = minerBanHistoryBuilder().empty
      historyInvariants(banHistory)
      val cancelledWarnings = Seq(CancelledWarning(74L, Warning(73L)),
                                  CancelledWarning(74L, Warning(72L)),
                                  CancelledWarning(74L, Warning(71L)),
                                  CancelledWarning(74L, Warning(70L)))
      val rolledBack = banHistory.rollback(71L, cancelledWarnings, MinerBanHistory.modernBanlistEntryOrdering)
      historyInvariants(rolledBack)

      rolledBack.hasWarnings shouldBe true
      rolledBack.entries shouldBe Seq(Warning(71L), Warning(70L))
    }

    "rollback part of cancelled warning" in {
      val banHistory = minerBanHistoryBuilder().empty
      historyInvariants(banHistory)
      val cancelledWarnings = Seq(CancelledWarning(75L, Warning(74L)),
                                  CancelledWarning(75L, Warning(73L)),
                                  CancelledWarning(72L, Warning(70L)),
                                  CancelledWarning(71L, Warning(69L)))
      val rolledBack1 = banHistory.rollback(71L, cancelledWarnings, MinerBanHistory.modernBanlistEntryOrdering)
      historyInvariants(rolledBack1)
      rolledBack1.hasWarnings shouldBe true
      rolledBack1.entries shouldBe Seq(Warning(70L))

      val rolledBack2 = banHistory.rollback(70L, cancelledWarnings, MinerBanHistory.modernBanlistEntryOrdering)
      historyInvariants(rolledBack2)
      rolledBack2.hasWarnings shouldBe true
      rolledBack2.entries shouldBe Seq(Warning(70L), Warning(69L))

      val rolledBack3 = banHistory.rollback(74L, cancelledWarnings, MinerBanHistory.modernBanlistEntryOrdering)
      historyInvariants(rolledBack3)
      rolledBack3.hasWarnings shouldBe true
      rolledBack3.entries shouldBe Seq(Warning(74L), Warning(73L))
    }
  }

  "Variable params" - {
    "vary warningsForBan" in {
      val banDurationBlocks = 100
      val warningsForBan    = 2

      val history = minerBanHistoryBuilder(banDurationBlocks, warningsForBan)
        .build(Seq(Warning(1L)))
        .warnOrBan(1000, 2L)

      history.isBanned(1001) shouldBe true
    }
  }

  def historyInvariants(history: MinerBanHistory): Assertion = {
    history.size shouldBe history.entries.size
    history.unsavedDepth should be <= history.size

    history.entries shouldBe history.entries.sorted
  }
}
