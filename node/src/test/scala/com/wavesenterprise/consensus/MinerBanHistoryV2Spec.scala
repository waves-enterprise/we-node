package com.wavesenterprise.consensus

import com.wavesenterprise.consensus.MinerBanlistEntry.{Ban, CancelledWarning, PriorWarningsInfo, Warning}

class MinerBanHistoryV2Spec extends MinerBanHistorySpecBase {

  override def minerBanHistoryBuilder(banDurationBlocks: Int, warningsForBan: Int): MinerBanHistoryBuilder = {
    new MinerBanHistoryBuilderV2(banDurationBlocks, warningsForBan)
  }

  "Unsaved depth is calculated correctly" in {
    val entries = Seq(Warning(1236L),
                      Warning(1235L),
                      Ban(1234L, 1, PriorWarningsInfo(Array(1233L, 1232L))),
                      Ban(1230L, 1, PriorWarningsInfo(Array(1229L, 1227L))))

    val history = minerBanHistoryBuilder().build(entries)
    history.unsavedDepth shouldBe 0

    val (_, historyWithoutWarning) = history.dropWarnings(1237L)
    historyWithoutWarning.unsavedDepth shouldBe 0

    history.warnOrBan(2, 1238L).unsavedDepth shouldBe 1

    history.rollback(1230, Seq.empty, MinerBanHistory.modernBanlistEntryOrdering).unsavedDepth shouldBe 0
    history.rollback(1234, Seq.empty, MinerBanHistory.modernBanlistEntryOrdering).unsavedDepth shouldBe 0
    history.rollback(1232, Seq.empty, MinerBanHistory.modernBanlistEntryOrdering).unsavedDepth shouldBe 1
    history.rollback(1233, Seq.empty, MinerBanHistory.modernBanlistEntryOrdering).unsavedDepth shouldBe 2

    history.rollback(1230, Seq(CancelledWarning(1231L, Warning(1228L))), MinerBanHistory.modernBanlistEntryOrdering).unsavedDepth shouldBe 1
    history.rollback(1234, Seq(CancelledWarning(1235L, Warning(1231L))), MinerBanHistory.modernBanlistEntryOrdering).unsavedDepth shouldBe 1
    history.rollback(1232, Seq(CancelledWarning(1233L, Warning(1231L))), MinerBanHistory.modernBanlistEntryOrdering).unsavedDepth shouldBe 2
    history.rollback(1233, Seq(CancelledWarning(1234L, Warning(1231L))), MinerBanHistory.modernBanlistEntryOrdering).unsavedDepth shouldBe 3
  }
}
