package com.wavesenterprise.state.diffs

import java.nio.charset.StandardCharsets.UTF_8

import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.features.FeatureProvider._
import com.wavesenterprise.state.{AssetInfo, Blockchain, ByteStr, Diff, LeaseBalance, Portfolio, SponsorshipValue}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.assets._
import com.wavesenterprise.transaction.{ProvenTransaction, ValidationError}

import scala.util.Right

object AssetTransactionsDiff {

  def issue(height: Int)(tx: IssueTransaction): Either[ValidationError, Diff] = {
    val asset = AssetInfo(
      issuer = tx.sender,
      height = height,
      timestamp = tx.timestamp,
      name = new String(tx.name, UTF_8),
      description = new String(tx.description, UTF_8),
      decimals = tx.decimals,
      reissuable = tx.reissuable,
      volume = tx.quantity
    )
    Right(
      Diff(
        height = height,
        tx = tx,
        portfolios = Map(tx.sender.toAddress -> Portfolio(balance = -tx.fee, lease = LeaseBalance.empty, assets = Map(tx.id() -> tx.quantity))),
        assets = Map(tx.id()                 -> asset),
        assetScripts = if (tx.script.isEmpty) { Map() } else { Map(tx.id() -> tx.script) }
      ))
  }

  def setAssetScript(blockchain: Blockchain, height: Int)(tx: SetAssetScriptTransactionV1): Either[ValidationError, Diff] = {
    for {
      _     <- Either.cond(tx.script.nonEmpty, (), GenericError("Cannot set empty script"))
      asset <- findAsset(blockchain, tx.assetId)
      _     <- validIssuer(tx, asset, issuerOnly = true)
      _     <- Either.cond(blockchain.hasAssetScript(tx.assetId), (), GenericError("Cannot set script on an asset issued without a script"))
    } yield
      Diff(
        height = height,
        tx = tx,
        portfolios = Map(tx.sender.toAddress -> Portfolio(balance = -tx.fee, lease = LeaseBalance.empty, assets = Map.empty)),
        assetScripts = Map(tx.assetId        -> tx.script)
      )
  }

  def reissue(blockchain: Blockchain, height: Int)(tx: ReissueTransaction): Either[ValidationError, Diff] = {
    val isDataTxActivated = blockchain.isFeatureActivated(BlockchainFeature.DataTransaction, height)

    for {
      asset <- findAsset(blockchain, tx.assetId)
      _     <- validIssuer(tx, asset, issuerOnly = true)
      _     <- Either.cond(asset.reissuable || (!isDataTxActivated && asset.wasBurnt), (), GenericError("Asset is not reissuable"))
      _     <- Either.cond(!((Long.MaxValue - tx.quantity) < asset.volume && isDataTxActivated), (), GenericError("Asset total value overflow"))
    } yield
      Diff(
        height = height,
        tx = tx,
        portfolios = Map(tx.sender.toAddress -> Portfolio(balance = -tx.fee, lease = LeaseBalance.empty, assets = Map(tx.assetId -> tx.quantity))),
        assets = Map(tx.assetId              -> asset.copy(volume = asset.volume + tx.quantity, reissuable = asset.reissuable && tx.reissuable))
      )
  }

  def burn(blockchain: Blockchain, height: Int)(tx: BurnTransaction): Either[ValidationError, Diff] = {
    val burnAnyTokensEnabled = blockchain.isFeatureActivated(BlockchainFeature.BurnAnyTokens, height)

    for {
      asset <- findAsset(blockchain, tx.assetId)
      _     <- validIssuer(tx, asset, !burnAnyTokensEnabled)
    } yield
      Diff(
        height = height,
        tx = tx,
        portfolios = Map(tx.sender.toAddress -> Portfolio(balance = -tx.fee, lease = LeaseBalance.empty, assets = Map(tx.assetId -> -tx.amount))),
        assets = Map(tx.assetId              -> asset.copy(volume = asset.volume - tx.amount, wasBurnt = true))
      )
  }

  def sponsor(blockchain: Blockchain, height: Int)(tx: SponsorFeeTransactionV1): Either[ValidationError, Diff] = {
    for {
      asset <- findAsset(blockchain, tx.assetId)
      _     <- validIssuer(tx, asset, issuerOnly = true)
      _     <- Either.cond(!blockchain.hasAssetScript(tx.assetId), (), GenericError("Cannot enable sponsorship for a smart asset"))
    } yield
      Diff(
        height = height,
        tx = tx,
        portfolios = Map(tx.sender.toAddress -> Portfolio(balance = -tx.fee, lease = LeaseBalance.empty, assets = Map.empty)),
        sponsorship = Map(tx.assetId         -> SponsorshipValue(tx.isEnabled))
      )
  }

  private def findAsset(blockchain: Blockchain, assetId: ByteStr): Either[ValidationError, AssetInfo] = {
    blockchain.asset(assetId).toRight(GenericError("Referenced assetId not found"))
  }

  private def validIssuer(tx: ProvenTransaction, asset: AssetInfo, issuerOnly: Boolean): Either[ValidationError, Unit] = {
    Either.cond(!issuerOnly || tx.sender == asset.issuer, (), GenericError("Asset was issued by other address"))
  }
}
