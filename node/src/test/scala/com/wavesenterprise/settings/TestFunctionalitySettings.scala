package com.wavesenterprise.settings

import com.wavesenterprise.features.BlockchainFeature

object TestFunctionalitySettings {
  val Enabled = FunctionalitySettings(
    featureCheckBlocksPeriod = 10000,
    blocksForFeatureActivation = 9000,
    preActivatedFeatures = Map(BlockchainFeature.SmartAccounts.id -> 0, BlockchainFeature.SmartAssets.id -> 0)
  )

  val EnabledForAtomics: FunctionalitySettings = Enabled.copy(
    preActivatedFeatures = Enabled.preActivatedFeatures ++ Map(
      BlockchainFeature.ContractsGrpcSupport.id     -> 0,
      BlockchainFeature.SponsoredFeesSupport.id     -> 0,
      BlockchainFeature.AtomicTransactionSupport.id -> 0
    )
  )

  val EnabledForContractValidation: FunctionalitySettings = Enabled.copy(
    preActivatedFeatures = Enabled.preActivatedFeatures ++ Map(
      BlockchainFeature.ContractsGrpcSupport.id       -> 0,
      BlockchainFeature.ContractValidationsSupport.id -> 0
    )
  )

  val EnabledForNativeTokens: FunctionalitySettings = Enabled.copy(
    preActivatedFeatures = Enabled.preActivatedFeatures ++ Map(
      BlockchainFeature.FeeSwitch.id                                 -> 0,
      BlockchainFeature.SponsoredFeesSupport.id                      -> 0,
      BlockchainFeature.ContractsGrpcSupport.id                      -> 0,
      BlockchainFeature.ContractNativeTokenSupportAndPkiV1Support.id -> 0,
      BlockchainFeature.ContractValidationsSupport.id                -> 0,
      BlockchainFeature.AtomicTransactionSupport.id                  -> 0
    ),
    featureCheckBlocksPeriod = 2,
    blocksForFeatureActivation = 1
  )

  val Stub: FunctionalitySettings = Enabled.copy(featureCheckBlocksPeriod = 100, blocksForFeatureActivation = 90)

  val EmptyFeaturesSettings: FeaturesSettings =
    FeaturesSettings(autoSupportImplementedFeatures = false, autoShutdownOnUnsupportedFeature = false, List.empty)
}
