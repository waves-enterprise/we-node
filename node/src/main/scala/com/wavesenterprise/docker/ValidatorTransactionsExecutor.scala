package com.wavesenterprise.docker

import cats.implicits._
import com.google.common.collect.Sets
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.docker.ContractExecutionStatus.Failure
import com.wavesenterprise.docker.grpc.GrpcDockerContractExecutor
import com.wavesenterprise.metrics.docker.ContractExecutionMetrics
import com.wavesenterprise.mining.{TransactionWithDiff, TransactionsAccumulator}
import com.wavesenterprise.network.{ContractValidatorResultsV1, ContractValidatorResultsV2}
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.crypto.internals.confidentialcontracts.Commitment
import com.wavesenterprise.database.rocksdb.confidential.ConfidentialRocksDBStorage
import com.wavesenterprise.docker.ValidatorTransactionsExecutor.ContractExecutionResultsId
import com.wavesenterprise.docker.grpc.service.ContractReadLogService
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.features.FeatureProvider.FeatureProviderExt
import com.wavesenterprise.state.{Blockchain, ByteStr, ContractId, DataEntry, NG}
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.{ConstraintsOverflowError, MvccConflictError}
import com.wavesenterprise.transaction.docker.ContractTransactionEntryOps.DataEntryMap
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation.ContractAssetOperationMap
import com.wavesenterprise.transaction.docker.{
  CallContractTransactionV6,
  ContractTransactionEntryOps,
  ContractTransactionValidation,
  ExecutableTransaction,
  ExecutedContractTransaction,
  ExecutedContractTransactionV1,
  ExecutedContractTransactionV3,
  ExecutedContractTransactionV4,
  ExecutedContractTransactionV5,
  ReadDescriptor,
  ReadingsHash
}
import com.wavesenterprise.utils.Time
import com.wavesenterprise.utils.pki.CrlCollection
import com.wavesenterprise.utx.UtxPool
import com.wavesenterprise.wasm.WASMContractExecutor
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
    val grpcContractExecutor: GrpcDockerContractExecutor,
    val wasmContractExecutor: WASMContractExecutor,
    val keyBlockId: ByteStr,
    val parallelism: Int,
    val readLogService: ContractReadLogService,
    val confidentialStorage: ConfidentialRocksDBStorage
)(implicit val scheduler: Scheduler)
    extends TransactionsExecutor {

  private val submittedResults = Sets.newConcurrentHashSet[ContractExecutionResultsId]()

  private[this] val contractNativeTokenFeatureActivated: Boolean =
    blockchain.isFeatureActivated(BlockchainFeature.ContractNativeTokenSupportAndPkiV1Support, blockchain.height)
  private[this] val leaseOpsForContractsFeatureActivated: Boolean = {
    blockchain.isFeatureActivated(BlockchainFeature.LeaseOpsForContractsSupport, blockchain.height)
  }
  private[this] val confidentialContractFeatureActivated: Boolean = {
    blockchain.isFeatureActivated(BlockchainFeature.ConfidentialDataInContractsSupport, blockchain.height)
  }

  override protected def handleUpdateSuccess(metrics: ContractExecutionMetrics,
                                             tx: ExecutableTransaction,
                                             maybeCertChainWithCrl: Option[(CertChain, CrlCollection)],
                                             atomically: Boolean): Either[ValidationError, TransactionWithDiff] = {
    (for {
      executedTx <- ExecutedContractTransactionV1.selfSigned(nodeOwnerAccount, tx, List.empty, time.getTimestamp())
      _          = log.debug(s"Built executed transaction '${executedTx.id()}' for '${tx.id()}'")
      contractId = ContractId(tx.contractId)
      // We need to check the validation policy before tx appending, because if the policy changes to any, we will not
      // send validation result and miner will not be able to process the tx.
      validationPolicyIsNotAnyBeforeAppend = validationPolicyIsNotAny(contractId)
      diff <- {
        if (atomically)
          transactionsAccumulator.processAtomically(executedTx, Seq.empty, maybeCertChainWithCrl)
        else
          transactionsAccumulator.process(executedTx, Seq.empty, maybeCertChainWithCrl)
      }
    } yield {
      def validationPolicyIsNotAnyAfterAppend: Boolean = validationPolicyIsNotAny(contractId)

      if (validationPolicyIsNotAnyBeforeAppend || validationPolicyIsNotAnyAfterAppend) {
        broadcastResultsMessage(tx, maybeConfidentialDataToBroadcast = None, List.empty, List.empty)
      }

      log.debug(s"Success update contract execution for tx '${tx.id()}'")
      TransactionWithDiff(executedTx, diff)
    }).leftMap { error =>
      handleExecutedTxError(tx)(error)
      error
    }
  }

  override protected def handleExecutionSuccess(results: List[DataEntry[_]],
                                                assetOperations: List[ContractAssetOperation],
                                                metrics: ContractExecutionMetrics,
                                                tx: ExecutableTransaction,
                                                maybeCertChainWithCrl: Option[(CertChain, CrlCollection)],
                                                atomically: Boolean): Either[ValidationError, TransactionWithDiff] = {
    (for {
      _ <- checkAssetOperationsSupported(contractNativeTokenFeatureActivated, assetOperations)
      _ <- checkLeaseOpsForContractSupported(leaseOpsForContractsFeatureActivated, assetOperations)

      resultsHash = ContractTransactionValidation.resultsHash(results, assetOperations)
      _ <- validateAssetIdLength(assetOperations)

      executedTxOutput <-
        extractInputCommitment(tx)
          .filter(_ => confidentialContractFeatureActivated)
          .map { inputCommitment =>
            buildConfidentialExecutedTx(results, tx, resultsHash, List.empty, inputCommitment)
          }.getOrElse {
            val executedTxOrError: Either[ValidationError, ExecutedContractTransaction] =
              if (contractNativeTokenFeatureActivated) {
                ExecutedContractTransactionV3.selfSigned(
                  nodeOwnerAccount,
                  tx,
                  results,
                  resultsHash,
                  List.empty,
                  time.getTimestamp(),
                  assetOperations
                )
              } else {
                ExecutedContractTransactionV1.selfSigned(nodeOwnerAccount, tx, results, time.getTimestamp())
              }

            executedTxOrError.map { executedTx =>
              ExecutedTxOutput(executedTx, Seq.empty)
            }
          }
      ExecutedTxOutput(executedTx, confidentialOutput) = executedTxOutput
      _                                                = log.debug(s"Built executed transaction '${executedTx.id()}' for '${tx.id()}'")
      diff <- {
        if (atomically)
          transactionsAccumulator.processAtomically(executedTx, confidentialOutput, maybeCertChainWithCrl)
        else
          transactionsAccumulator.process(executedTx, confidentialOutput, maybeCertChainWithCrl)
      }
    } yield {
      if (validationPolicyIsNotAny(ContractId(tx.contractId))) {
        val maybeConfidentialDataToBroadcast = executedTx match {
          case tx: ExecutedContractTransactionV4 => Some(ConfidentialDataToBroadcast(tx.readings, tx.readingsHash, tx.outputCommitment))
          case _                                 => None
        }
        broadcastResultsMessage(tx, maybeConfidentialDataToBroadcast = maybeConfidentialDataToBroadcast, results, assetOperations)
      }
      log.debug(s"Success contract execution for tx '${tx.id()}'")
      TransactionWithDiff(executedTx, diff)
    }).leftMap { error =>
      handleExecutedTxError(tx)(error)
      error
    }
  }

  override protected def handleExecutionSuccess(results: ContractTransactionEntryOps.DataEntryMap,
                                                assetOperations: ContractAssetOperation.ContractAssetOperationMap,
                                                metrics: ContractExecutionMetrics,
                                                tx: ExecutableTransaction,
                                                maybeCertChainWithCrl: Option[(CertChain, CrlCollection)],
                                                atomically: Boolean): Either[ValidationError, TransactionWithDiff] = {
    val assetOperationsList = assetOperations.mapping.values.flatten.toList
    val resultsHash         = ContractTransactionValidation.resultsMapHash(results, assetOperations)
    (for {
      _        <- validateAssetIdLength(assetOperationsList)
      txPolicy <- transactionsAccumulator.validationPolicy(tx)
      calledContracts = results.mapping.keys ++ assetOperations.mapping.keys
      allPolicies = Seq(txPolicy) ++ calledContracts.map(ContractId)
        .flatMap(transactionsAccumulator.contract).map(_.validationPolicy)
      validationPolicy <- deriveValidationPolicy(tx.contractId, allPolicies)
      executedTxOutput <- extractInputCommitment(tx)
        .map { inputCommitment =>
          buildConfidentialExecutedTx(results, tx, resultsHash, List.empty, inputCommitment)
        }.getOrElse {

          ExecutedContractTransactionV5.selfSigned(
            nodeOwnerAccount,
            tx,
            results,
            resultsHash,
            List.empty,
            time.getTimestamp(),
            readings = List.empty,
            readingsHash = None,
            outputCommitmentOpt = None,
            assetOperations,
            0,
            None,
          ).map(ExecutedTxOutput(_, Seq.empty))

        }
      ExecutedTxOutput(executedTx: ExecutedContractTransactionV5, confidentialOutput) = executedTxOutput
      _                                                                               = log.debug(s"Built executed transaction '${executedTx.id()}' for '${tx.id()}'")
      diff <- {
        if (atomically)
          transactionsAccumulator.processAtomically(executedTx, confidentialOutput, maybeCertChainWithCrl)
        else
          transactionsAccumulator.process(executedTx, confidentialOutput, maybeCertChainWithCrl)
      }
    } yield {
      if (validationPolicy != ValidationPolicy.Any) {
        broadcastResultsMessage(
          tx,
          executedTx.outputCommitmentOpt.map(com => ConfidentialDataToBroadcast(executedTx.readings, executedTx.readingsHash, com)),
          results,
          assetOperations
        )
      }
      log.debug(s"Success contract execution for tx '${tx.id()}'")
      TransactionWithDiff(executedTx, diff)
    }).leftMap { error =>
      handleExecutedTxError(tx)(error)
      error
    }
  }

  override protected def handleExecutionError(statusCode: Int,
                                              errorMessage: String,
                                              metrics: ContractExecutionMetrics,
                                              tx: ExecutableTransaction,
                                              maybeCertChainWithCrl: Option[(CertChain, CrlCollection)],
                                              atomically: Boolean,
                                              txContext: TxContext.TxContext): Either[ValidationError, TransactionWithDiff] = {
    val results     = DataEntryMap(Map.empty)
    val operations  = ContractAssetOperationMap(Map.empty)
    val resultsHash = ContractTransactionValidation.resultsMapHash(results, operations)
    (for {
      executedTx <- ExecutedContractTransactionV5.selfSigned(
        nodeOwnerAccount,
        tx,
        results,
        resultsHash,
        List.empty,
        time.getTimestamp(),
        readings = List.empty,
        readingsHash = None,
        outputCommitmentOpt = None,
        operations,
        statusCode,
        Some(errorMessage),
      )
      _ = log.debug(s"Built executed transaction '${executedTx.id()}' for '${tx.id()}'")
      diff <- {
        if (atomically)
          transactionsAccumulator.processAtomically(executedTx, Seq.empty, maybeCertChainWithCrl)
        else
          transactionsAccumulator.process(executedTx, Seq.empty, maybeCertChainWithCrl)
      }
    } yield {
      broadcastResultsMessage(tx, None, results, operations)
      log.debug(s"Success contract execution for tx '${tx.id()}'")
      TransactionWithDiff(executedTx, diff)
    }).leftMap { error =>
      handleExecutedTxError(tx)(error)
      error
    }
  }

  override protected def enrichStatusMessage(message: String): String = s"Validator error: $message"

  private def handleExecutedTxError(tx: ExecutableTransaction): Function[ValidationError, Unit] = {
    case _: ConstraintsOverflowError =>
      log.debug(s"Executed transaction for '${tx.id()}' was discarded because it exceeds the constraints")
    case MvccConflictError =>
      log.debug(s"Executed transaction for '${tx.id()}' was discarded because it caused MVCC conflict")
      mvccConflictCounter.increment()
    case error =>
      val message = s"Executed transaction creation error: '$error'"
      utx.remove(tx, Some(message), mustBeInPool = true)
      log.error(s"$message for tx '${tx.id()}'")
      val enrichedMessage = enrichStatusMessage(message)
      messagesCache.put(tx.id(), ContractExecutionMessage(nodeOwnerAccount, tx.id(), Failure, None, enrichedMessage, time.correctedTime()))
  }

  private def isConfidential(tx: ExecutableTransaction): Boolean = tx match {
    case tx: CallContractTransactionV6 if blockchain.contract(ContractId(tx.contractId)).exists(_.isConfidential) => true
    case _                                                                                                        => false
  }

  private case class ConfidentialDataToBroadcast(readings: Seq[ReadDescriptor], readingsHash: Option[ReadingsHash], outputCommitment: Commitment)

  private def broadcastResultsMessage(tx: ExecutableTransaction,
                                      maybeConfidentialDataToBroadcast: Option[ConfidentialDataToBroadcast],
                                      results: List[DataEntry[_]],
                                      assetOps: List[ContractAssetOperation]): Unit = {
    val message = if (isConfidential(tx)) {
      val ConfidentialDataToBroadcast(readings, readingsHash, outputCommitment) =
        maybeConfidentialDataToBroadcast.getOrElse(
          throw new IllegalStateException(s"Failed getting readings, readingsHash, output commitment for tx: ${tx.id()}"))
      ContractValidatorResultsV2(nodeOwnerAccount, tx.id(), keyBlockId, readings, readingsHash, outputCommitment, results, assetOps)
    } else ContractValidatorResultsV1(nodeOwnerAccount, tx.id(), keyBlockId, results, assetOps)

    val resultsId = ContractExecutionResultsId(message.txId, message.resultsHash)

    if (!submittedResults.contains(resultsId)) {
      val currentMiner = blockchain.currentMiner
      val minerChannel = currentMiner.flatMap(activePeerConnections.channelForAddress)

      (currentMiner, minerChannel) match {
        case (Some(miner), None) =>
          log.trace(s"Current miner '$miner' is not connected peer. Broadcasting validator results to all active peers")
          activePeerConnections.broadcast(message)
          submittedResults.add(resultsId)
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
          submittedResults.add(resultsId)
        case _ => log.error("Current miner is unknown")
      }
    }
  }

  private def broadcastResultsMessage(tx: ExecutableTransaction,
                                      maybeConfidentialDataToBroadcast: Option[ConfidentialDataToBroadcast],
                                      results: DataEntryMap,
                                      assetOps: ContractAssetOperationMap): Unit = {
    val message = if (isConfidential(tx)) {
      val ConfidentialDataToBroadcast(readings, readingsHash, outputCommitment) =
        maybeConfidentialDataToBroadcast.getOrElse(
          throw new IllegalStateException(s"Failed getting readings, readingsHash, output commitment for tx: ${tx.id()}"))
      ContractValidatorResultsV2(nodeOwnerAccount, tx.id(), keyBlockId, readings, readingsHash, outputCommitment, results, assetOps)
    } else ContractValidatorResultsV1(nodeOwnerAccount, tx.id(), keyBlockId, results, assetOps)
    val resultsId = ContractExecutionResultsId(message.txId, message.resultsHash)

    if (!submittedResults.contains(resultsId)) {
      val currentMiner = blockchain.currentMiner
      val minerChannel = currentMiner.flatMap(activePeerConnections.channelForAddress)

      (currentMiner, minerChannel) match {
        case (Some(miner), None) =>
          log.trace(s"Current miner '$miner' is not connected peer. Broadcasting validator results to all active peers")
          activePeerConnections.broadcast(message)
          submittedResults.add(resultsId)
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
          submittedResults.add(resultsId)
        case _ => log.error("Current miner is unknown")
      }
    }
  }

  @inline
  private def validationPolicyIsNotAny(contractId: ContractId): Boolean = {
    transactionsAccumulator
      .contract(contractId)
      .exists(_.validationPolicy != ValidationPolicy.Any)
  }

}

object ValidatorTransactionsExecutor {

  private case class ContractExecutionResultsId(txId: ByteStr, resultsHash: ByteStr)

}
