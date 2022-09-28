package com.wavesenterprise.state.diffs

import com.wavesenterprise.state._
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.transaction.{DataTransaction, ValidationError}

object DataTransactionDiff {

  def apply(blockchain: Blockchain, height: Int)(tx: DataTransaction): Either[ValidationError, Diff] = {
    val sender = tx.sender.toAddress
    val author = tx.author.toAddress
    Right(
      Diff(
        height,
        tx,
        portfolios = Diff.feeAssetIdPortfolio(tx, author.toAssetHolder, blockchain),
        accountData = Map(sender -> AccountDataInfo(tx.data.map(item => item.key -> item).toMap))
      ))
  }
}
