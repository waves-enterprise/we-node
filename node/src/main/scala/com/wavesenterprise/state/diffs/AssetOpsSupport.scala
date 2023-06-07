package com.wavesenterprise.state.diffs

import cats.implicits._
import com.wavesenterprise.state.reader.LeaseDetails
import com.wavesenterprise.state.{
  Account,
  AssetInfo,
  Blockchain,
  ByteStr,
  Contract,
  ContractId,
  Diff,
  LeaseBalance,
  LeaseId,
  Portfolio,
  SponsorshipValue
}
import com.wavesenterprise.transaction.ValidationError.{GenericError, InvalidAssetId}
import com.wavesenterprise.transaction.assets._
import com.wavesenterprise.transaction.docker.ExecutedContractTransactionV3
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation
import com.wavesenterprise.transaction.smart.script.Script
import com.wavesenterprise.transaction.{AssetId, AssetIdLength, ProvenTransaction, ValidationError}

import java.nio.charset.StandardCharsets.UTF_8

trait AssetOpsSupport {
  import com.wavesenterprise.state.AssetHolder._

  protected def findAssetAndCheckCallerGrants(
      blockchain: Blockchain,
      tx: ProvenTransaction,
      assetId: AssetId,
      onlyIssuerAccepted: Boolean
  ): Either[ValidationError, AssetInfo] =
    for {
      asset <- findAsset(blockchain, assetId)
      _     <- validIssuer(tx, asset, onlyIssuerAccepted)
    } yield asset

  protected def findAsset(blockchain: Blockchain, assetId: AssetId): Either[ValidationError, AssetInfo] =
    blockchain.asset(assetId).toRight(GenericError("Referenced assetId not found"))

  protected def findAssetForContract(
      blockchain: Blockchain,
      currentDiff: Diff,
      assetId: AssetId
  ): Either[ValidationError, AssetInfo] = {
    blockchain
      .asset(assetId)
      .orElse(currentDiff.assets.get(assetId))
      .toRight(GenericError(s"Referenced assetId '$assetId' doesn't exist"))
  }

  protected def validIssuer(
      tx: ProvenTransaction,
      asset: AssetInfo,
      issuerOnly: Boolean
  ): Either[ValidationError, Unit] = {
    val senderAddress = tx.sender.toAddress
    asset.issuer match {
      case Account(assetIssuer) =>
        Either.cond(!issuerOnly || senderAddress == assetIssuer, (), GenericError("Asset was issued by other address"))
      case Contract(contractId) =>
        Left(GenericError(s"Asset was issued by contract '$contractId' and cannot be modified by '$senderAddress'"))
    }
  }

  protected def checkAssetNotExist(blockchain: Blockchain, assetId: ByteStr): Either[ValidationError, Unit] =
    blockchain.asset(assetId) match {
      case Some(_) => GenericError(s"Asset '$assetId' already exists").asLeft
      case None    => ().asRight
    }

  protected def checkLeaseIdNotExist(blockchain: Blockchain, leaseId: ByteStr): Either[ValidationError, Unit] =
    blockchain.leaseDetails(LeaseId(leaseId)) match {
      case Some(_) => GenericError(s"Lease '$leaseId' already exists").asLeft
      case None    => ().asRight
    }

  protected def checkLeaseActive(lease: LeaseDetails): Either[GenericError, Unit] = {
    if (!lease.isActive) {
      Left(GenericError(s"Cannot cancel already cancelled lease"))
    } else Right(())
  }

  protected def checkOverflowAfterReissue(
      asset: AssetInfo,
      additionalQuantity: Long,
      isDataTxActivated: Boolean
  ): Either[ValidationError, Unit] =
    Either.cond(!((Long.MaxValue - additionalQuantity) < asset.volume && isDataTxActivated), (), GenericError("Asset total value overflow"))

  protected def checkAssetCanBeReissued(asset: AssetInfo): Either[ValidationError, Unit] =
    Either.cond(asset.reissuable, (), GenericError("Asset is not reissuable"))

  def checkAssetIdLength(assetId: AssetId): Either[ValidationError, Unit] = {
    Either.cond(
      test = assetId.arr.length == AssetIdLength,
      right = (),
      left = InvalidAssetId(s"Invalid assetId length. Current - '${assetId.arr.length}', expected – '$AssetIdLength'")
    )
  }

  def checkLeaseIdLength(leaseId: ByteStr): Either[ValidationError, Unit] = {
    val requiredLength = com.wavesenterprise.crypto.DigestSize

    Either.cond(
      test = leaseId.arr.length == requiredLength,
      right = (),
      left = InvalidAssetId(s"Invalid assetId length. Current - '${leaseId.arr.length}', expected – '$requiredLength'")
    )
  }

  protected def assetInfoFromIssueTransaction(tx: IssueTransaction, height: Int): AssetInfo =
    AssetInfo(
      issuer = tx.sender.toAddress.toAssetHolder,
      height = height,
      timestamp = tx.timestamp,
      name = new String(tx.name, UTF_8),
      description = new String(tx.description, UTF_8),
      decimals = tx.decimals,
      reissuable = tx.reissuable,
      volume = tx.quantity
    )

  protected def diffFromIssueTransaction(tx: IssueTransaction, asset: AssetInfo): Diff =
    Diff(
      height = asset.height,
      tx = tx,
      portfolios = Map(tx.sender.toAddress.toAssetHolder -> Portfolio(-tx.fee, LeaseBalance.empty, Map(tx.id() -> tx.quantity))),
      assets = Map(tx.id() -> asset),
      assetScripts = tx.script.fold(Map.empty[AssetId, Option[Script]])(_ => Map(tx.id() -> tx.script))
    )

  protected def diffFromSetScriptTransaction(tx: SetAssetScriptTransactionV1, height: Int): Diff =
    Diff(
      height = height,
      tx = tx,
      portfolios = Map(tx.sender.toAddress.toAssetHolder -> Portfolio(-tx.fee, LeaseBalance.empty, Map.empty)),
      assetScripts = Map(tx.assetId -> tx.script)
    )

  protected def diffFromReissueTransaction(tx: ReissueTransaction, asset: AssetInfo, height: Int): Diff =
    Diff(
      height = height,
      tx = tx,
      portfolios = Map(tx.sender.toAddress.toAssetHolder -> Portfolio(-tx.fee, LeaseBalance.empty, Map(tx.assetId -> tx.quantity))),
      assets = Map(tx.assetId -> asset.copy(volume = asset.volume + tx.quantity, reissuable = asset.reissuable && tx.reissuable))
    )

  protected def diffFromBurnTransaction(tx: BurnTransaction, asset: AssetInfo, height: Int): Diff =
    Diff(
      height = height,
      tx = tx,
      portfolios = Map(tx.sender.toAddress.toAssetHolder -> Portfolio(-tx.fee, LeaseBalance.empty, Map(tx.assetId -> -tx.amount))),
      assets = Map(tx.assetId -> asset.copy(volume = asset.volume - tx.amount))
    )

  protected def diffFromSponsorFeeTransaction(tx: SponsorFeeTransaction, height: Int): Diff =
    Diff(
      height = height,
      tx = tx,
      portfolios = Map(tx.sender.toAddress.toAssetHolder -> Portfolio(-tx.fee, LeaseBalance.empty, Map.empty)),
      sponsorship = Map(tx.assetId -> SponsorshipValue(tx.isEnabled))
    )

  protected def diffFromContractIssue(tx: ExecutedContractTransactionV3, issueOp: ContractAssetOperation.ContractIssueV1, height: Int): Diff = {
    val assetInfo = AssetInfo(
      issuer = ContractId(tx.tx.contractId).toAssetHolder,
      height = height,
      timestamp = tx.timestamp,
      name = issueOp.name,
      description = issueOp.description,
      decimals = issueOp.decimals,
      reissuable = issueOp.isReissuable,
      volume = issueOp.quantity
    )
    Diff(
      height = height,
      tx = tx,
      portfolios =
        Map(ContractId(tx.tx.contractId).toAssetHolder -> Portfolio(0, LeaseBalance.empty, Map(issueOp.assetId -> assetInfo.volume.longValue()))),
      assets = Map(issueOp.assetId -> assetInfo)
    )
  }

  protected def diffFromContractReissue(tx: ExecutedContractTransactionV3,
                                        reissueOp: ContractAssetOperation.ContractReissueV1,
                                        asset: AssetInfo,
                                        height: Int): Diff =
    Diff(
      height = height,
      tx = tx,
      portfolios = Map(ContractId(tx.tx.contractId).toAssetHolder -> Portfolio(0, LeaseBalance.empty, Map(reissueOp.assetId -> reissueOp.quantity))),
      assets =
        Map(reissueOp.assetId -> asset.copy(volume = asset.volume + reissueOp.quantity, reissuable = asset.reissuable && reissueOp.isReissuable))
    )

  protected def diffFromContractBurn(tx: ExecutedContractTransactionV3,
                                     burnOp: ContractAssetOperation.ContractBurnV1,
                                     asset: AssetInfo,
                                     height: Int): Either[ValidationError, Diff] = {
    burnOp.assetId match {
      case None =>
        Left(GenericError("Burning WEST is not allowed"))
      case Some(assetId) =>
        Diff(
          height = height,
          tx = tx,
          portfolios = Map(ContractId(tx.tx.contractId).toAssetHolder -> Portfolio(0, LeaseBalance.empty, Map(assetId -> -burnOp.amount))),
          assets = Map(assetId -> asset.copy(volume = asset.volume - burnOp.amount))
        ).asRight
    }
  }
}
