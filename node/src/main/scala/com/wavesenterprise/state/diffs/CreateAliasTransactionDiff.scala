package com.wavesenterprise.state.diffs

import com.wavesenterprise.state.{Blockchain, Diff}
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.{CreateAliasTransaction, ValidationError}

import scala.util.Right

object CreateAliasTransactionDiff {
  def apply(blockchain: Blockchain, height: Int)(tx: CreateAliasTransaction): Either[ValidationError, Diff] = {
    if (blockchain.aliasesIssuedByAddress(tx.sender.toAddress).nonEmpty)
      Left(GenericError("Only one alias per address is allowed"))
    else if (!blockchain.canCreateAlias(tx.alias))
      Left(GenericError("Alias already claimed"))
    else
      Right(
        Diff(
          height = height,
          tx = tx,
          portfolios = Diff.feeAssetIdPortfolio(tx, tx.sender.toAddress.toAssetHolder, blockchain),
          aliases = Map(tx.alias -> tx.sender.toAddress)
        ))
  }
}
