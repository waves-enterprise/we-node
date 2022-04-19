package com.wavesenterprise.state.diffs

import cats.implicits._
import com.wavesenterprise.account.Address
import com.wavesenterprise.state._
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.{GenericError, Validation}
import com.wavesenterprise.transaction.transfer.ParsedTransfer
import com.wavesenterprise.transaction.transfer._

object MassTransferTransactionDiff {

  def apply(blockchain: Blockchain, blockTime: Long, height: Int)(tx: MassTransferTransaction): Either[ValidationError, Diff] = {
    def parseTransfer(xfer: ParsedTransfer): Validation[(Map[Address, Portfolio], Long)] = {
      for {
        recipientAddr <- blockchain.resolveAlias(xfer.recipient)
        portfolio = tx.assetId match {
          case None      => Map(recipientAddr -> Portfolio(xfer.amount, LeaseBalance.empty, Map.empty))
          case Some(aid) => Map(recipientAddr -> Portfolio(0, LeaseBalance.empty, Map(aid -> xfer.amount)))
        }
      } yield (portfolio, xfer.amount)
    }
    val portfoliosEi = tx.transfers.traverse(parseTransfer)

    portfoliosEi.flatMap { list: List[(Map[Address, Portfolio], Long)] =>
      val sender = Address.fromPublicKey(tx.sender.publicKey)
      val (recipientPortfolios, totalAmount) = list
        .reduceOption { (u, v) =>
          (u._1 combine v._1, u._2 + v._2)
        }
        .getOrElse((Map.empty[Address, Portfolio], 0L))

      val completePortfolio = recipientPortfolios
        .combine(tx.assetId match {
          case None      => Map(sender -> Portfolio(-totalAmount, LeaseBalance.empty, Map.empty))
          case Some(aid) => Map(sender -> Portfolio(0, LeaseBalance.empty, Map(aid -> -totalAmount)))
        })
        .combine(Diff.feeAssetIdPortfolio(tx, sender, blockchain))

      val assetIssued = tx.assetId.forall(blockchain.assetDescription(_).isDefined)
      val feeAssetIssued = tx.feeAssetId match {
        case Some(feeAssetId) => blockchain.assetDescription(feeAssetId).isDefined
        case _                => true
      }
      val isValid = assetIssued && feeAssetIssued

      Either.cond(isValid, Diff(height, tx, completePortfolio), GenericError(s"Attempt to transfer a nonexistent asset"))
    }
  }
}
