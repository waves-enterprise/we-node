package com.wavesenterprise.api

import cats.data.EitherT
import cats.implicits._
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.api.http.ApiError.{CustomValidationError, PolicyItemDataIsMissing}
import com.wavesenterprise.network.TxBroadcaster
import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.privacy.PolicyStorage
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.transaction.docker.{
  CallContractTransactionV7,
  CreateContractTransactionV7,
  ExecutableTransaction,
  UpdateContractTransactionV6
}
import com.wavesenterprise.transaction.validation.{ExecutableValidation, FeeCalculator}
import com.wavesenterprise.transaction.{AtomicTransaction, PolicyDataHashTransaction, Transaction}
import monix.eval.Task

trait TransactionsApi {
  def blockchain: Blockchain
  def feeCalculator: FeeCalculator
  def txBroadcaster: TxBroadcaster
  def policyStorage: PolicyStorage

  def validateAndBroadcastTransaction(tx: Transaction, certChain: Option[CertChain] = None): EitherT[Task, ApiError, Transaction] = {
    for {
      _ <- EitherT.fromEither[Task](feeCalculator.validateTxFee(blockchain.height, tx)).leftMap(ApiError.fromValidationError)
      _ <- additionalBroadcastValidation(tx)
      _ <- txBroadcaster.broadcastIfNew(tx, certChain).leftMap(ApiError.fromValidationError)
      _ <- EitherT.fromEither[Task](unimplementedApiValidation(tx))
    } yield tx
  }

  protected def additionalBroadcastValidation(
      tx: Transaction,
      atomicTransactions: Seq[Transaction] = Seq.empty
  ): EitherT[Task, ApiError, Transaction] = {
    tx match {
      case tx: PolicyDataHashTransaction =>
        EitherT {
          policyStorage.policyItemExists(tx.policyId.toString, tx.dataHash.toString)
        }.flatMap { inStorage =>
          EitherT.fromEither[Task] {
            Either.cond(inStorage, tx, PolicyItemDataIsMissing(tx.dataHash.toString))
          }
        }
      case atomicTx: AtomicTransaction =>
        atomicTx.transactions
          .traverse(additionalBroadcastValidation(_, atomicTx.transactions))
          .as(atomicTx)
      case executableTx: ExecutableTransaction =>
        EitherT(Task(ExecutableValidation.validateApiVersion(executableTx, blockchain, atomicTransactions).leftMap(ApiError.fromValidationError)))
      case _ =>
        EitherT.rightT[Task, ApiError](tx)
    }
  }

  private def unimplementedApiValidation(
      tx: Transaction
  ): Either[ApiError, Transaction] = {
    tx match {
      case create: CreateContractTransactionV7 =>
        if (create.isConfidential)
          Left(CustomValidationError(s"CSC support is not enabled yet for transaction: $create"))
        else Right(create)
      case update: UpdateContractTransactionV6 =>
        if (update.groupParticipants.nonEmpty || update.groupOwners.nonEmpty)
          Left(CustomValidationError(s"CSC support is not enabled yet for transaction: $update"))
        else Right(update)
      case atomic: AtomicTransaction =>
        val unimplemented = atomic.transactions.filter {
          case _: UpdateContractTransactionV6 | _: CreateContractTransactionV7 | _: CallContractTransactionV7 =>
            true
          case _ =>
            false
        }
        if (unimplemented.isEmpty)
          Right(tx)
        else
          Left(CustomValidationError(s"Atomic support is not enabled yet for following transactions: $unimplemented"))

      case _ => Right(tx)
    }

  }
}
