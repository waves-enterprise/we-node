package com.wavesenterprise.consensus

import com.wavesenterprise.consensus.MinerBanlistEntry.{Ban, PriorWarningsInfo}

class MinerBanHistoryV1Spec extends MinerBanHistorySpecBase {

  override def minerBanHistoryBuilder(banDurationBlocks: Int, warningsForBan: Int): MinerBanHistoryBuilder = {
    new MinerBanHistoryBuilderV1(banDurationBlocks, warningsForBan)
  }

  "History contains only unique values" in {
    val sample               = Ban(1, 2, PriorWarningsInfo(Array(1, 2, 3)))
    val historyWithDuplicate = MinerBanHistoryV1(Seq(sample, sample), 1, 1)

    historyWithDuplicate.entries.length shouldBe 1
  }
}
