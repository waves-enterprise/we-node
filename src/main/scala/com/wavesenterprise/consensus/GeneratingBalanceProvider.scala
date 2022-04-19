package com.wavesenterprise.consensus

import com.wavesenterprise.account.Address
import com.wavesenterprise.settings.FunctionalitySettings
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.utils.NumberUtils.DoubleExt

object GeneratingBalanceProvider {
  val MinimalEffectiveBalance: Long = 10000.west
  val BalanceDepth                  = 1000

  def balance(blockchain: Blockchain, fs: FunctionalitySettings, height: Int, account: Address): Long =
    balance(blockchain, height, account)

  def balance(blockchain: Blockchain, height: Int, account: Address): Long =
    blockchain.effectiveBalance(account, height, BalanceDepth)
}
