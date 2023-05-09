package com.wavesenterprise.state.diffs

import cats._
import cats.implicits._
import com.wavesenterprise.account.Address
import com.wavesenterprise.settings.FunctionalitySettings
import com.wavesenterprise.state._
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.lease._
import AssetHolder._
import com.wavesenterprise.state.reader.LeaseDetails

import scala.util.{Left, Right}

object LeaseTransactionsDiff {

  def lease(blockchain: Blockchain, height: Int)(tx: LeaseTransaction): Either[ValidationError, Diff] = {
    val sender = Address.fromPublicKey(tx.sender.publicKey)
    blockchain.resolveAlias(tx.recipient).flatMap { recipient =>
      if (recipient == sender)
        Left(GenericError(s"Cannot lease to self. Sender address $sender equals to recipient address"))
      else {
        val ap = blockchain.addressPortfolio(tx.sender.toAddress)
        if (ap.balance - ap.lease.out < tx.amount) {
          Left(GenericError(s"Cannot lease more than own: Balance:${ap.balance}, already leased: ${ap.lease.out}"))
        } else {
          val portfolioDiff: Map[Address, Portfolio] = Map(
            sender    -> Portfolio(-tx.fee, LeaseBalance(0, tx.amount), Map.empty),
            recipient -> Portfolio(0, LeaseBalance(tx.amount, 0), Map.empty)
          )
          val leaseDetails = LeaseDetails.fromLeaseTx(tx, height)

          Right(Diff(height = height, tx = tx, portfolios = portfolioDiff.toAssetHolderMap, leaseMap = Map(LeaseId(tx.id()) -> leaseDetails)))
        }
      }
    }
  }

  def leaseCancel(blockchain: Blockchain, settings: FunctionalitySettings, time: Long, height: Int)(
      tx: LeaseCancelTransaction): Either[ValidationError, Diff] = {
    val leaseEi = blockchain.leaseDetails(LeaseId(tx.leaseId)) match {
      case None    => Left(GenericError(s"Related LeaseTransaction not found"))
      case Some(l) => Right(l)
    }
    for {
      lease     <- leaseEi
      recipient <- blockchain.resolveAlias(lease.recipient)
      isLeaseActive = lease.isActive
      _ <- if (!isLeaseActive)
        Left(GenericError(s"Cannot cancel already cancelled lease"))
      else
        Right(())
      canceller = Address.fromPublicKey(tx.sender.publicKey)
      portfolioDiff <- if (tx.sender == lease.sender) {
        Right(
          Monoid.combine(Map(canceller -> Portfolio(-tx.fee, LeaseBalance(0, -lease.amount), Map.empty)),
                         Map(recipient -> Portfolio(0, LeaseBalance(-lease.amount, 0), Map.empty))))
      } else {
        Left(GenericError(s"LeaseTransaction was leased by other sender"))
      }
      leaseDetails = lease.copy(isActive = false)
    } yield Diff(height = height, tx = tx, portfolios = portfolioDiff.toAssetHolderMap, leaseMap = Map(LeaseId(tx.leaseId) -> leaseDetails))
  }
}
