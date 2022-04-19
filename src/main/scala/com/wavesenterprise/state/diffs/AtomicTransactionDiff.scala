package com.wavesenterprise.state.diffs

import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.state.reader.CompositeBlockchain.composite
import com.wavesenterprise.state.{Blockchain, Diff}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.validation.AtomicValidation.validateInnerTransactions
import com.wavesenterprise.transaction.{AtomicTransaction, Signed, ValidationError}

case class AtomicTransactionDiff(blockchain: Blockchain,
                                 differ: TransactionDiffer,
                                 height: Int,
                                 blockOpt: Option[Signed],
                                 minerOpt: Option[PublicKeyAccount]) {

  def apply(tx: AtomicTransaction): Either[ValidationError, Diff] = {
    validateInnerTransactions(tx.transactions, tx.miner, tx.sender.toAddress) >>
      ((blockOpt, minerOpt) match {
        case (Some(block), _)    => validateMiner(tx, block.sender)
        case (None, Some(miner)) => validateMiner(tx, miner)
        case _                   => Right(())
      }) >> calcDiff(tx)
  }

  private def validateMiner(tx: AtomicTransaction, blockMiner: PublicKeyAccount): Either[ValidationError, Unit] = {
    tx.miner match {
      case Some(miner) =>
        Either.cond(
          miner == blockMiner,
          (),
          GenericError(s"Atomic transaction must be mined only by block creator: miner is '$miner', but block creator is '$blockMiner'")
        )
      case None => Left(GenericError("Atomic transaction validation failed: miner is empty"))
    }
  }

  private def calcDiff(tx: AtomicTransaction): Either[ValidationError, Diff] = {
    val initial = Diff(height = height, tx = tx, portfolios = Diff.feeAssetIdPortfolio(tx, tx.sender.toAddress, blockchain))
    tx.transactions.foldLeft(initial.asRight[ValidationError]) {
      case (acc, tx) =>
        for {
          accDiff <- acc
          updatedBlockchain = composite(blockchain, accDiff)
          diff <- differ(updatedBlockchain, tx, atomically = true)
        } yield {
          accDiff.combine(diff)
        }
    }
  }
}
