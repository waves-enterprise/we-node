package com.wavesenterprise.mining

import cats.data.NonEmptyList
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.features.FeatureProvider._
import com.wavesenterprise.settings.MinerSettings
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.block.Block

case class MiningConstraints(total: MiningConstraint, keyBlock: MiningConstraint, micro: MiningConstraint)

object MiningConstraints {
  val MaxScriptRunsInBlock              = 100
  private val ClassicAmountOfTxsInBlock = Block.MaxTransactionsPerBlockVer1Ver2

  def apply(blockchain: Blockchain, height: Int, maxBlockSizeInBytes: Int, minerSettings: Option[MinerSettings] = None): MiningConstraints = {
    val activatedFeatures     = blockchain.activatedFeaturesAt(height)
    val isNgEnabled           = activatedFeatures.contains(BlockchainFeature.NG.id)
    val isMassTransferEnabled = activatedFeatures.contains(BlockchainFeature.MassTransfer.id)
    val isScriptEnabled       = activatedFeatures.contains(BlockchainFeature.SmartAccounts.id)

    val totalConstraint = {
      val blockSizeConstraint: MiningConstraint = if (isMassTransferEnabled) {
        OneDimensionalMiningConstraint(maxBlockSizeInBytes, TxEstimators.sizeInBytes)
      } else {
        val maxTxs = if (isNgEnabled) Block.MaxTransactionsPerBlockVer3 else ClassicAmountOfTxsInBlock
        OneDimensionalMiningConstraint(maxTxs, TxEstimators.one)
      }

      if (isScriptEnabled) {
        MultiDimensionalMiningConstraint(
          NonEmptyList.of(OneDimensionalMiningConstraint(MaxScriptRunsInBlock, TxEstimators.scriptRunNumber), blockSizeConstraint))
      } else {
        blockSizeConstraint
      }
    }

    val keyBlockConstraint = {
      if (isNgEnabled) {
        if (isMassTransferEnabled) {
          OneDimensionalMiningConstraint(0, TxEstimators.one)
        } else {
          minerSettings
            .map(_ => OneDimensionalMiningConstraint(0, TxEstimators.one))
            .getOrElse(MiningConstraint.Unlimited)
        }
      } else {
        OneDimensionalMiningConstraint(ClassicAmountOfTxsInBlock, TxEstimators.one)
      }
    }

    val microBlockConstraint = if (isNgEnabled && minerSettings.isDefined) {
      OneDimensionalMiningConstraint(minerSettings.get.maxTransactionsInMicroBlock, TxEstimators.one, isCriticalConstraint = true)
    } else {
      MiningConstraint.Unlimited
    }

    new MiningConstraints(
      total = totalConstraint,
      keyBlock = keyBlockConstraint,
      micro = microBlockConstraint
    )
  }
}
