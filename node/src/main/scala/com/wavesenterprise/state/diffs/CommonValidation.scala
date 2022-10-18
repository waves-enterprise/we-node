package com.wavesenterprise.state.diffs

import cats._
import cats.implicits._
import com.wavesenterprise.account.Address
import com.wavesenterprise.features.FeatureProvider._
import com.wavesenterprise.state.{Blockchain, LeaseBalance, Portfolio}
import com.wavesenterprise.transaction.ValidationError._
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.docker.assets.ContractTransferInV1
import com.wavesenterprise.transaction.docker.{ExecutableTransaction, ExecutedContractTransactionV3}
import com.wavesenterprise.transaction.transfer._

import scala.concurrent.duration._
import scala.util.{Left, Right}

object CommonValidation {
  val MaxTimeTransactionOverBlockDiff: FiniteDuration     = 90.minutes
  val MaxTimePrevBlockOverTransactionDiff: FiniteDuration = 2.hours

  /**
    * Checks if sender is trying to spend more than owns
    */
  def disallowSendingGreaterThanBalance[T <: Transaction](blockchain: Blockchain, tx: T): Either[ValidationError, T] = {

    def checkSpendings(sender: Address,
                       assetId: Option[AssetId],
                       amount: Long,
                       feeAssetId: Option[AssetId],
                       feeAmount: Long): Either[ValidationError, T] = {
      val spendings: Portfolio = Monoid.combine(
        assetId match {
          case Some(aid) => Portfolio(0, LeaseBalance.empty, Map(aid -> -amount))
          case None      => Portfolio(-amount, LeaseBalance.empty, Map.empty)
        },
        feeAssetId match {
          case Some(aid) => Portfolio(0, LeaseBalance.empty, Map(aid -> -feeAmount))
          case None      => Portfolio(-feeAmount, LeaseBalance.empty, Map.empty)
        }
      )

      val senderPortfolio = blockchain.addressPortfolio(sender)

      val oldWestBalance = senderPortfolio.balance
      val newWestBalance = oldWestBalance + spendings.balance

      if (newWestBalance < 0) {
        Left(
          GenericError(
            "Attempt to transfer unavailable funds: Transaction application leads to " +
              s"negative WEST balance to (at least) temporary negative state, current balance equals $oldWestBalance, " +
              s"spends equals ${spendings.balance}, result is $newWestBalance"))
      } else if (spendings.assets.nonEmpty) {
        val oldAssetBalances = senderPortfolio.assets
        val balanceError = spendings.assets.collectFirst {
          case (assetId, deltaToSpend) if oldAssetBalances.getOrElse(assetId, 0L) + deltaToSpend < 0 =>
            val availableBalance = oldAssetBalances.getOrElse(assetId, 0L)
            GenericError(
              "Attempt to transfer unavailable funds: Transaction application leads to negative asset " +
                s"'$assetId' balance to (at least) temporary negative state, current balance is $availableBalance, " +
                s"spends equals $deltaToSpend, result is ${availableBalance + deltaToSpend}")
        }

        balanceError.toLeft(tx)
      } else Right(tx)
    }

    /**
      * Validate that user owns enough assets to make these payments to a contract and pay the fees
      */
    def validateContractPayments(senderPortfolio: Portfolio,
                                 payments: List[ContractTransferInV1],
                                 feeAssetId: Option[AssetId],
                                 feeAmount: Long): Either[ValidationError, T] = {
      val paymentsGrouped: Map[Option[AssetId], Long] = payments.groupBy(_.assetId).mapValues(_.map(_.amount).sum)
      val maybeErrors = paymentsGrouped.foldLeft(List.empty[ValidationError]) {
        case (maybeErrors, (assetIdOpt, requestedAmount)) =>
          val ownedAmount = assetIdOpt.fold(senderPortfolio.balance)(assetId => senderPortfolio.assets.getOrElse(assetId, 0L))

          if (ownedAmount < requestedAmount) {
            InsufficientFunds(assetIdOpt, ownedAmount, requestedAmount) :: maybeErrors
          } else {
            maybeErrors
          }
      }

      val paymentsValidationResult: Either[ValidationError, T] = maybeErrors match {
        case Nil =>
          Right(tx)
        case errs =>
          Left(GenericError(s"Attempt to transfer unavailable funds. Errors: [${errs.mkString("; ")}]"))
      }

      val feeValidationResult = {
        val requiredAssetAmount = paymentsGrouped.getOrElse(feeAssetId, 0L) + feeAmount
        val ownedAmount         = feeAssetId.fold(senderPortfolio.balance)(assetId => senderPortfolio.assets(assetId))
        Either.cond(ownedAmount >= requiredAssetAmount, tx, InsufficientFunds(feeAssetId, ownedAmount, requiredAssetAmount))
      }

      paymentsValidationResult >> feeValidationResult
    }

    tx match {
      case transfer: TransferTransaction =>
        checkSpendings(transfer.sender.toAddress, transfer.assetId, transfer.amount, transfer.feeAssetId, transfer.fee)

      case massTransfer: MassTransferTransaction =>
        checkSpendings(massTransfer.sender.toAddress,
                       massTransfer.assetId,
                       massTransfer.transfers.map(_.amount).sum,
                       massTransfer.feeAssetId,
                       massTransfer.fee)

      case executableTx: ExecutableTransaction with PaymentsV1ToContract =>
        val sender          = executableTx.sender.toAddress
        val senderPortfolio = blockchain.addressPortfolio(sender)
        validateContractPayments(senderPortfolio, executableTx.payments, executableTx.feeAssetId, executableTx.fee)

      case executedTx: ExecutedContractTransactionV3 =>
        disallowSendingGreaterThanBalance(blockchain, executedTx.tx) >> Right(tx)

      case authorizedTx: Authorized =>
        checkSpendings(authorizedTx.sender.toAddress, None, 0L, tx.feeAssetId, tx.fee)
        Right(tx)

      // case for the genesis txs which don't have a sender/author
      case _ =>
        Right(tx)
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
