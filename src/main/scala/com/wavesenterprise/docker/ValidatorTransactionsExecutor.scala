package com.wavesenterprise.docker

import cats.implicits._
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.docker.ContractExecutionStatus.Failure
import com.wavesenterprise.docker.grpc.GrpcContractExecutor
import com.wavesenterprise.metrics.docker.ContractExecutionMetrics
import com.wavesenterprise.mining.{TransactionWithDiff, TransactionsAccumulator}
import com.wavesenterprise.network.ContractValidatorResults
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.state.{Blockchain, ByteStr, DataEntry, NG}
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.{ConstraintsOverflowError, MvccConflictError}
import com.wavesenterprise.transaction.docker.{ExecutableTransaction, ExecutedContractTransactionV1}
import com.wavesenterprise.utils.Time
import com.wavesenterprise.utx.UtxPool
import io.netty.channel.group.ChannelGroupFuture
import monix.execution.Scheduler

class ValidatorTransactionsExecutor(
    val nodeOwnerAccount: PrivateKeyAccount,
    val transactionsAccumulator: TransactionsAccumulator,
    val messagesCache: ContractExecutionMessagesCache,
    val utx: UtxPool,
    val blockchain: Blockchain with NG,
    val time: Time,
    val activePeerConnections: ActivePeerConnections,
    val legacyContractExecutor: LegacyContractExecutor,
    val grpcContractExecutor: GrpcContractExecutor,
    val keyBlockId: ByteStr,
    val parallelism: Int
)(implicit val scheduler: Scheduler)
    extends TransactionsExecutor {

  override protected def handleUpdateSuccess(metrics: ContractExecutionMetrics,
                                             tx: ExecutableTransaction,
                                             atomically: Boolean): Either[ValidationError, TransactionWithDiff] = {
    (for {
      executedTx <- ExecutedContractTransactionV1.selfSigned(nodeOwnerAccount, tx, List.empty, time.getTimestamp())
      _ = log.debug(s"Built executed transaction '${executedTx.id()}' for '${tx.id()}'")
      diff <- if (atomically) transactionsAccumulator.processAtomically(executedTx) else transactionsAccumulator.process(executedTx)
    } yield {
      broadcastResultsMessage(tx, List.empty)
      log.debug(s"Success update contract execution for tx '${tx.id()}'")
      TransactionWithDiff(executedTx, diff)
    }).leftMap { error =>
      handleExecutedTxError(tx)(error)
      error
    }
  }

  override protected def handleExecutionSuccess(results: List[DataEntry[_]],
                                                metrics: ContractExecutionMetrics,
                                                tx: ExecutableTransaction,
                                                atomically: Boolean): Either[ValidationError, TransactionWithDiff] = {
    val changedResults = onlyChangedResults(tx, results)
    (for {
      executedTx <- ExecutedContractTransactionV1.selfSigned(nodeOwnerAccount, tx, changedResults, time.getTimestamp())
      _ = log.debug(s"Built executed transaction '${executedTx.id()}' for '${tx.id()}'")
      diff <- if (atomically) transactionsAccumulator.processAtomically(executedTx) else transactionsAccumulator.process(executedTx)
    } yield {
      broadcastResultsMessage(tx, changedResults)
      log.debug(s"Success contract execution for tx '${tx.id()}'")
      TransactionWithDiff(executedTx, diff)
    }).leftMap { error =>
      handleExecutedTxError(tx)(error)
      error
    }
  }

  override protected def enrichStatusMessage(message: String): String = s"Validator error: $message"

  private def handleExecutedTxError(tx: ExecutableTransaction): Function[ValidationError, Unit] = {
    case ConstraintsOverflowError =>
      log.debug(s"Executed transaction for '${tx.id()}' was discarded because it exceeds the constraints")
      ConstraintsOverflowError
    case MvccConflictError =>
      log.debug(s"Executed transaction for '${tx.id()}' was discarded because it caused MVCC conflict")
      MvccConflictError
    case error =>
      val message = s"Executed transaction creation error: '$error'"
      utx.remove(tx, Some(message), mustBeInPool = true)
      log.error(s"$message for tx '${tx.id()}'")
      val enrichedMessage = enrichStatusMessage(message)
      messagesCache.put(tx.id(), ContractExecutionMessage(nodeOwnerAccount, tx.id(), Failure, None, enrichedMessage, time.correctedTime()))
  }

  private def broadcastResultsMessage(tx: ExecutableTransaction, results: List[DataEntry[_]]): Unit = {
    val message = ContractValidatorResults(nodeOwnerAccount, tx.id(), keyBlockId, results)

    val currentMiner = blockchain.currentMiner
    val minerChannel = currentMiner.flatMap(activePeerConnections.channelForAddress)

    (currentMiner, minerChannel) match {
      case (Some(miner), None) =>
        log.trace(s"Current miner '$miner' is not connected peer. Broadcasting validator results to all active peers")
        activePeerConnections.broadcast(message)
      case (Some(miner), Some(channel)) =>
        log.trace(s"Broadcasting validator results to current miner '$miner'")
        activePeerConnections.broadcastTo(message, Set(channel)).addListener { (future: ChannelGroupFuture) =>
          {
            Option(future.cause()).foreach { ex =>
              log.warn(s"Failed to broadcast message to '${channel.remoteAddress().toString}'. Retrying to broadcast to all active peers", ex)
              activePeerConnections.broadcast(message)
            }
          }
        }
      case _ => log.error("Current miner is unknown")
    }
  }
}
