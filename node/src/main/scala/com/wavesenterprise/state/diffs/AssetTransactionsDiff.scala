package com.wavesenterprise.state.diffs

import cats.implicits._
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.features.FeatureProvider._
import com.wavesenterprise.state.{Blockchain, Diff}
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.assets._

object AssetTransactionsDiff extends AssetOpsSupport {

  def issue(height: Int)(tx: IssueTransaction): Either[ValidationError, Diff] =
    diffFromIssueTransaction(tx, assetInfoFromIssueTransaction(tx, height)).asRight[ValidationError]

  def setAssetScript(blockchain: Blockchain, height: Int)(tx: SetAssetScriptTransactionV1): Either[ValidationError, Diff] =
    for {
      _ <- tx.script.toRight(GenericError("Cannot set empty script"))
      _ <- findAssetAndCheckCallerGrants(blockchain, tx, tx.assetId, onlyIssuerAccepted = true)
      _ <- blockchain.assetScript(tx.assetId).toRight(GenericError("Cannot set script on an asset issued without a script"))
    } yield diffFromSetScriptTransaction(tx, height)

  def reissue(blockchain: Blockchain, height: Int)(tx: ReissueTransaction): Either[ValidationError, Diff] = {
    val isDataTxActivated = blockchain.isFeatureActivated(BlockchainFeature.DataTransaction, height)

    for {
      asset <- findAssetAndCheckCallerGrants(blockchain, tx, tx.assetId, onlyIssuerAccepted = true)
      _     <- checkAssetCanBeReissued(asset)
      _     <- checkOverflowAfterReissue(asset, tx.quantity, isDataTxActivated)
    } yield diffFromReissueTransaction(tx, asset, height)
  }

  def burn(blockchain: Blockchain, height: Int)(tx: BurnTransaction): Either[ValidationError, Diff] = {
    val burnAnyTokensEnabled = blockchain.isFeatureActivated(BlockchainFeature.BurnAnyTokens, height)

    for {
      asset <- findAssetAndCheckCallerGrants(blockchain, tx, tx.assetId, onlyIssuerAccepted = !burnAnyTokensEnabled)
    } yield diffFromBurnTransaction(tx, asset, height)
  }

  def sponsor(blockchain: Blockchain, height: Int)(tx: SponsorFeeTransaction): Either[ValidationError, Diff] =
    for {
      _ <- findAssetAndCheckCallerGrants(blockchain, tx, tx.assetId, onlyIssuerAccepted = true)
      _ <- Either.cond(!blockchain.hasAssetScript(tx.assetId), (), GenericError("Cannot enable sponsorship for a smart asset"))
    } yield diffFromSponsorFeeTransaction(tx, height)
}
