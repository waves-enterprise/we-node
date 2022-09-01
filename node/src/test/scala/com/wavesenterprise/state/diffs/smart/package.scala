package com.wavesenterprise.state.diffs

import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.settings.{FunctionalitySettings, TestFunctionalitySettings}

package object smart {
  val smartEnabledFS: FunctionalitySettings =
    TestFunctionalitySettings.Enabled.copy(
      preActivatedFeatures =
        Map(BlockchainFeature.SmartAccounts.id -> 0, BlockchainFeature.SmartAssets.id -> 0, BlockchainFeature.DataTransaction.id -> 0))
}
