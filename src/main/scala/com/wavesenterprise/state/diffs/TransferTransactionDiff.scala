package com.wavesenterprise.state.diffs

import cats.implicits._
import com.wavesenterprise.account.Address
import com.wavesenterprise.settings.FunctionalitySettings
import com.wavesenterprise.state._
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.transfer._

import scala.util.Try

object TransferTransactionDiff {

  def apply(blockchain: Blockchain, settings: FunctionalitySettings, blockTime: Long, height: Int)(
      tx: TransferTransaction): Either[ValidationError, Diff] = {
    val sender = Address.fromPublicKey(tx.sender.publicKey)

    for {
      recipient <- blockchain.resolveAlias(tx.recipient)
      _         <- Either.cond(tx.assetId.forall(blockchain.assetDescription(_).isDefined), (), GenericError(s"Unissued asset id are not allowed"))
      _         <- Either.cond(tx.feeAssetId.forall(blockchain.assetDescription(_).isDefined), (), GenericError(s"Unissued fee asset id are not allowed"))
      _ <- Either.cond((tx.feeAssetId >>= blockchain.assetDescription >>= (_.script)).isEmpty,
                       (),
                       GenericError("Smart assets can't participate in TransferTransactions as a fee"))
      _ <- validateOverflow(tx)
    } yield {
      val transferPortfolios = tx.assetId match {
        case None =>
          Map(sender      -> Portfolio(-tx.amount, LeaseBalance.empty, Map.empty)) |+|
            Map(recipient -> Portfolio(tx.amount, LeaseBalance.empty, Map.empty))
        case Some(aid) =>
          Map(sender      -> Portfolio(0, LeaseBalance.empty, Map(aid -> -tx.amount))) |+|
            Map(recipient -> Portfolio(0, LeaseBalance.empty, Map(aid -> tx.amount)))
      }

      val feePortfolios = Diff.feeAssetIdPortfolio(tx, sender, blockchain)
      val allPortfolios = transferPortfolios |+| feePortfolios

      Diff(height, tx, allPortfolios)
    }
  }

  private def validateOverflow(tx: TransferTransaction): Either[ValidationError, Unit] = {
    Try(Math.addExact(tx.fee, tx.amount))
      .fold(
        _ => ValidationError.OverflowError.asLeft[Unit],
        _ => ().asRight[ValidationError]
      )
  }
}
