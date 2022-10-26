package com.wavesenterprise.state.diffs

import cats.implicits._
import com.wavesenterprise.account.Address
import com.wavesenterprise.settings.FunctionalitySettings
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state._
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.transfer._

object TransferTransactionDiff extends TransferOpsSupport {
  private lazy val SmartAssetASFeeError = GenericError("Smart assets can't participate in TransferTransactions as a fee")

  def apply(blockchain: Blockchain, settings: FunctionalitySettings, blockTime: Long, height: Int)(
      tx: TransferTransaction): Either[ValidationError, Diff] = {
    val sender = Address.fromPublicKey(tx.sender.publicKey)

    for {
      recipient <- blockchain.resolveAlias(tx.recipient).map(_.toAssetHolder)
      _         <- validateAssetExistence(blockchain, tx.assetId)
      _         <- validateAssetExistence(blockchain, tx.feeAssetId, isFee = true)
      _         <- Either.cond((tx.feeAssetId >>= blockchain.assetDescription >>= (_.script)).isEmpty, (), SmartAssetASFeeError)
      _         <- validateOverflow(tx)
    } yield Diff(height, tx, getPortfoliosMap(tx, sender.toAssetHolder, recipient) |+| Diff.feeAssetIdPortfolio(tx, sender.toAssetHolder, blockchain))
  }
}
