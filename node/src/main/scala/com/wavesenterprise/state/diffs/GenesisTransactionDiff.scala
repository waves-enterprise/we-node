package com.wavesenterprise.state.diffs

import com.wavesenterprise.state.{Diff, LeaseBalance, Portfolio}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.{GenesisTransaction, ValidationError}
import com.wavesenterprise.state.AssetHolder._

import scala.util.{Left, Right}

object GenesisTransactionDiff {
  def apply(height: Int)(tx: GenesisTransaction): Either[ValidationError, Diff] = {
    if (height != 1) Left(GenericError("GenesisTransaction cannot appear in non-initial block"))
    else
      Right(
        Diff(height = height,
             tx = tx,
             portfolios = Map(tx.recipient.toAssetHolder -> Portfolio(balance = tx.amount, LeaseBalance.empty, assets = Map.empty))))
  }
}
