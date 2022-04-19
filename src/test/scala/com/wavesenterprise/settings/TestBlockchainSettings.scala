package com.wavesenterprise.settings

import com.wavesenterprise.history.settings
import com.wavesenterprise.settings.TestFeeUtils.TestFeeExt

object TestBlockchainSettings {

  val Default: BlockchainSettings = BlockchainSettings(
    Custom(TestFunctionalitySettings.Enabled, settings.blockchain.custom.genesis, settings.blockchain.custom.addressSchemeCharacter),
    TestFees.defaultFees.toFeeSettings,
    ConsensusSettings.PoSSettings
  )

  def withFunctionality(fs: FunctionalitySettings): BlockchainSettings =
    Default.copy(custom = Default.custom.copy(functionality = fs))

  def withGenesis(genesisSettings: GenesisSettings): BlockchainSettings =
    Default.copy(custom = Default.custom.copy(genesis = genesisSettings))

}
