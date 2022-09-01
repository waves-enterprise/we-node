package com.wavesenterprise.state.diffs

import cats._
import cats.implicits._
import com.wavesenterprise.account.Address
import com.wavesenterprise.features.FeatureProvider._
import com.wavesenterprise.state.{Blockchain, LeaseBalance, Portfolio}
import com.wavesenterprise.transaction.ValidationError._
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.transfer._

import scala.concurrent.duration._
import scala.util.{Left, Right}

object CommonValidation {

  val MaxTimeTransactionOverBlockDiff: FiniteDuration     = 90.minutes
  val MaxTimePrevBlockOverTransactionDiff: FiniteDuration = 2.hours

  def disallowSendingGreaterThanBalance[T <: Transaction](blockchain: Blockchain, tx: T): Either[ValidationError, T] = {
    def checkTransfer(sender: Address, assetId: Option[AssetId], amount: Long, feeAssetId: Option[AssetId], feeAmount: Long) = {
      val amountDiff = assetId match {
        case Some(aid) => Portfolio(0, LeaseBalance.empty, Map(aid -> -amount))
        case None      => Portfolio(-amount, LeaseBalance.empty, Map.empty)
      }
      val feeDiff = feeAssetId match {
        case Some(aid) => Portfolio(0, LeaseBalance.empty, Map(aid -> -feeAmount))
        case None      => Portfolio(-feeAmount, LeaseBalance.empty, Map.empty)
      }

      val spendings      = Monoid.combine(amountDiff, feeDiff)
      val oldWestBalance = blockchain.portfolio(sender).balance

      val newWestBalance = oldWestBalance + spendings.balance
      if (newWestBalance < 0) {
        Left(
          GenericError(
            "Attempt to transfer unavailable funds: Transaction application leads to " +
              s"negative WEST balance to (at least) temporary negative state, current balance equals $oldWestBalance, " +
              s"spends equals ${spendings.balance}, result is $newWestBalance"))
      } else if (spendings.assets.nonEmpty) {
        val oldAssetBalances = blockchain.portfolio(sender).assets
        val balanceError = spendings.assets.collectFirst {
          case (aid, delta) if oldAssetBalances.getOrElse(aid, 0L) + delta < 0 =>
            val availableBalance = oldAssetBalances.getOrElse(aid, 0L)
            GenericError(
              "Attempt to transfer unavailable funds: Transaction application leads to negative asset " +
                s"'$aid' balance to (at least) temporary negative state, current balance is $availableBalance, " +
                s"spends equals $delta, result is ${availableBalance + delta}")
        }

        balanceError.fold[Either[ValidationError, T]](Right(tx))(Left(_))
      } else Right(tx)
    }

    tx match {
      case ttx: TransferTransaction     => checkTransfer(ttx.sender.toAddress, ttx.assetId, ttx.amount, ttx.feeAssetId, ttx.fee)
      case mtx: MassTransferTransaction => checkTransfer(mtx.sender.toAddress, mtx.assetId, mtx.transfers.map(_.amount).sum, mtx.feeAssetId, mtx.fee)
      case _                            => Right(tx)
    }
  }

  def disallowDuplicateIds(blockchain: Blockchain, tx: Transaction): Either[ValidationError, Transaction] = {
    Either.cond(!blockchain.containsTransaction(tx), tx, AlreadyInTheState(tx.id(), blockchain.transactionInfo(tx.id()).get._1))
  }

  def disallowBeforeActivationTime[T <: Transaction](blockchain: Blockchain, height: Int, tx: T): Either[ValidationError, Unit] = {
    tx.requiredFeatures.toList.traverse { feature =>
      Either.cond(
        blockchain.isFeatureActivated(feature, height),
        tx,
        ValidationError.ActivationError(
          s"Blockchain feature '${feature.description}' (id: '${feature.id}') has not been activated yet, but is required for ${tx.getClass.getSimpleName}")
      )
    }.void
  }

  def disallowTxFromFuture[T <: Transaction](time: Long, tx: T): Either[ValidationError, T] = {
    if (tx.timestamp - time > MaxTimeTransactionOverBlockDiff.toMillis)
      Left(Mistiming(s"Transaction ts ${tx.timestamp} is from far future. BlockTime: $time"))
    else Right(tx)
  }

  def disallowTxFromPast[T <: Transaction](prevBlockTime: Option[Long], tx: T, txExpireTimeout: FiniteDuration): Either[ValidationError, T] =
    prevBlockTime match {
      case Some(t) if (t - tx.timestamp) > txExpireTimeout.toMillis =>
        Left(Mistiming(s"Transaction ts ${tx.timestamp} is too old. Previous block time: $prevBlockTime"))
      case _ => Right(tx)
    }
}
