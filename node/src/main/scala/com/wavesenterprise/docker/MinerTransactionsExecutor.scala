package com.wavesenterprise.docker

import cats.implicits._
import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.block.{MicroBlock, TxMicroBlock}
import com.wavesenterprise.docker.grpc.GrpcContractExecutor
import com.wavesenterprise.docker.validator.{ContractValidatorResultsStore, ValidationPolicy}
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.features.FeatureProvider.FeatureProviderExt
import com.wavesenterprise.metrics.docker.{ContractExecutionMetrics, CreateExecutedTx, ProcessContractTx}
import com.wavesenterprise.mining.{ContractValidatorResultsOps, TransactionWithDiff, TransactionsAccumulator}
import com.wavesenterprise.network.{ConfidentialInventory, ContractValidatorResults, ContractValidatorResultsV2}
import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.database.rocksdb.confidential.ConfidentialRocksDBStorage
import com.wavesenterprise.docker.grpc.service.ContractReadLogService
import com.wavesenterprise.network.contracts.{ConfidentialDataInventoryBroadcaster, ConfidentialDataType}
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.state.{Blockchain, ByteStr, DataEntry, NG}
import com.wavesenterprise.transaction.ValidationError.{ConstraintsOverflowError, InvalidValidationProofs, MvccConflictError}
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation
import com.wavesenterprise.transaction.{AtomicTransaction, Transaction, ValidationError}
import com.wavesenterprise.utils.Time
import com.wavesenterprise.utils.pki.CrlCollection
import com.wavesenterprise.utx.UtxPool
import monix.execution.Scheduler
import com.wavesenterprise.state.contracts.confidential.ConfidentialOutput

import java.util.concurrent.ConcurrentHashMap

class MinerTransactionsExecutor(
    val messagesCache: ContractExecutionMessagesCache,
    val transactionsAccumulator: TransactionsAccumulator,
    val nodeOwnerAccount: PrivateKeyAccount,
    val utx: UtxPool,
    val blockchain: Blockchain with NG,
    val time: Time,
    val grpcContractExecutor: GrpcContractExecutor,
    val contractValidatorResultsStore: ContractValidatorResultsStore,
    val keyBlockId: ByteStr,
    val parallelism: Int,
    val confidentialStorage: ConfidentialRocksDBStorage,
    val peers: ActivePeerConnections,
    val readLogService: ContractReadLogService
)(implicit val scheduler: Scheduler)
    extends TransactionsExecutor with ConfidentialDataInventoryBroadcaster with ContractValidatorResultsOps {

  import ContractExecutionStatus._

  private[this] val minerAddress = nodeOwnerAccount.toAddress

  private[this] val txMetrics = new ConcurrentHashMap[ByteStr, ContractExecutionMetrics]()
  private[this] val validationFeatureActivated: Boolean =
    blockchain.isFeatureActivated(BlockchainFeature.ContractValidationsSupport, blockchain.height)
  private[this] val contractNativeTokenFeatureActivated: Boolean = {
    blockchain.isFeatureActivated(BlockchainFeature.ContractNativeTokenSupportAndPkiV1Support, blockchain.height)
  }
  private[this] val leaseOpsForContractsFeatureActivated: Boolean = {
    blockchain.isFeatureActivated(BlockchainFeature.LeaseOpsForContractsSupport, blockchain.height)
  }
  private[this] val confidentialContractFeatureActivated: Boolean = {
    blockchain.isFeatureActivated(BlockchainFeature.ConfidentialDataInContractsSupport, blockchain.height)
  }

  override def contractValidatorResultsStoreOpt: Option[ContractValidatorResultsStore] = contractValidatorResultsStore.some

  contractValidatorResultsStore.removeExceptFor(keyBlockId)

  def selectExecutableTxPredicate(tx: ExecutableTransaction,
                                  accumulatedValidationPolicies: Map[ByteStr, ValidationPolicy] = Map.empty,
                                  confidentialGroupParticipants: Set[Address] = Set.empty): Boolean =
    !validationFeatureActivated || enoughProofs(tx, accumulatedValidationPolicies, confidentialGroupParticipants)

  private def enoughProofs(tx: ExecutableTransaction,
                           accumulatedValidationPolicies: Map[ByteStr, ValidationPolicy],
                           confidentialGroupParticipants: Set[Address]): Boolean = {
    (accumulatedValidationPolicies.get(tx.contractId) orElse transactionsAccumulator.validationPolicy(tx).toOption)
      .exists {
        case ValidationPolicy.Any => true
        case ValidationPolicy.Majority =>
          checkProofsMajority(tx.id(), requiredAddresses = Set.empty, confidentialGroupParticipants = confidentialGroupParticipants)
        case ValidationPolicy.MajorityWithOneOf(addresses) =>
          checkProofsMajority(tx.id(), addresses.toSet, confidentialGroupParticipants = confidentialGroupParticipants)
      }
  }

  private def checkProofsMajority(txId: ByteStr, requiredAddresses: Set[Address], confidentialGroupParticipants: Set[Address]): Boolean = {
    if (confidentialGroupParticipants.isEmpty) {
      checkProofsMajority(txId, requiredAddresses)
    } else {
      checkProofsMajorityConfidential(txId, requiredAddresses, confidentialGroupParticipants)
    }
  }

  private def checkProofsMajority(txId: ByteStr, requiredAddresses: Set[Address]): Boolean = {
    val validators       = blockchain.lastBlockContractValidators - minerAddress
    val validatorResults = contractValidatorResultsStore.findResults(keyBlockId, txId, validators)

    validators.nonEmpty && validatorResults.nonEmpty && {
      val bestGroup    = validatorResults.groupBy(r => r.txId -> r.resultsHash).values.maxBy(_.size)
      val majoritySize = math.ceil(ValidationPolicy.MajorityRatio * validators.size).toInt

      val requiredAddressesCondition = requiredAddresses.isEmpty || (requiredAddresses intersect bestGroup.map(_.sender.toAddress)).nonEmpty
      val majorityCondition          = bestGroup.size >= majoritySize

      def groupDetails = bestGroup.map(r => r.sender.toAddress -> r.resultsHash)
      log.trace(
        s"Exist '${bestGroup.size}' validator proofs of '${validators.size}' for tx '$txId': '$groupDetails'." +
          s"RequiredAddresses: $requiredAddressesCondition, Majority: $majorityCondition")

      requiredAddressesCondition && majorityCondition
    }
  }

  private def checkProofsMajorityConfidential(txId: ByteStr,
                                              requiredAddresses: Set[Address],
                                              confidentialGroupParticipants: Set[Address]): Boolean = {
    val validators = confidentialGroupParticipants intersect blockchain.lastBlockContractValidators - minerAddress
    val validatorResults = contractValidatorResultsStore.findResults(keyBlockId, txId, validators)
      .collect { case resultsV2: ContractValidatorResultsV2 => resultsV2 }

    validators.nonEmpty && validatorResults.nonEmpty && {
      val bestGroup =
        validatorResults.groupBy(r => r.txId -> r.resultsHash -> r.readings -> r.readingsHash -> r.outputCommitment).values.maxBy(_.size)
      val majoritySize = math.ceil(ValidationPolicy.MajorityRatio * validators.size).toInt

      val requiredAddressesCondition = requiredAddresses.isEmpty || (requiredAddresses intersect bestGroup.map(_.sender.toAddress)).nonEmpty
      val majorityCondition          = bestGroup.size >= majoritySize

      def groupDetails = bestGroup.map(r => r.sender.toAddress -> r.resultsHash -> r.readings -> r.readingsHash -> r.outputCommitment)

      log.trace(
        s"Exist '${bestGroup.size}' validator proofs of '${validators.size}' for tx '$txId': '$groupDetails'." +
          s"RequiredAddresses: $requiredAddressesCondition, Majority: $majorityCondition")

      requiredAddressesCondition && majorityCondition
    }
  }

  def onMicroBlockMined(microBlock: MicroBlock): Unit =
    microBlock match {
      case txMicro: TxMicroBlock =>
        txMicro.transactionData
          .flatMap {
            case executedContractTransaction: ExecutedContractTransaction => List(executedContractTransaction)
            case atomicTransaction: AtomicTransaction                     => extractAtomicExecutedTxs(atomicTransaction)
            case _                                                        => Nil
          }
          .foreach(markTransactionMined)
      case _ => ()
    }

  private def extractAtomicExecutedTxs(container: AtomicTransaction): List[ExecutedContractTransaction] =
    container.transactions.flatMap {
      case executedTx: ExecutedContractTransaction => List(executedTx)
      case _                                       => Nil
    }

  private def markTransactionMined(executedTx: ExecutedContractTransaction): Unit = {
    log.debug(s"Remove mined executed transaction '${executedTx.id()}' from executed list")
    Option(txMetrics.remove(executedTx.id())).foreach(_.markContractTxMined())
    val executableTx = executedTx.tx
    messagesCache.put(
      executableTx.id(),
      ContractExecutionMessage(nodeOwnerAccount, executableTx.id(), Success, None, "Contract transaction successfully mined", time.correctedTime())
    )
  }

  override protected def handleExecutionSuccess(
      results: List[DataEntry[_]],
      assetOperations: List[ContractAssetOperation],
      metrics: ContractExecutionMetrics,
      tx: ExecutableTransaction,
      maybeCertChainWithCrl: Option[(CertChain, CrlCollection)],
      atomically: Boolean
  ): Either[ValidationError, TransactionWithDiff] =
    (validateAssetIdLength(assetOperations) >> createExecutedTx(results, assetOperations, metrics, tx))
      .leftMap { error =>
        handleExecutedTxCreationFailed(tx)(error)
        error
      }
      .flatMap { case ExecutedTxOutput(tx, maybeConfidentialOutput) =>
        log.debug(s"Built executed transaction '${tx.id()}' for '${tx.tx.id()}'")
        maybeConfidentialOutput.foreach(processConfidentialOutput)
        processExecutedTx(tx, metrics, maybeCertChainWithCrl, maybeConfidentialOutput = maybeConfidentialOutput, atomically)
      }

  private def createExecutedTx(
      results: List[DataEntry[_]],
      assetOperations: List[ContractAssetOperation],
      metrics: ContractExecutionMetrics,
      tx: ExecutableTransaction
  ): Either[ValidationError, ExecutedTxOutput] =
    if (validationFeatureActivated) {
      metrics.measureEither(
        CreateExecutedTx,
        for {
          validationPolicy <- transactionsAccumulator.validationPolicy(tx)
          resultsHash = ContractTransactionValidation.resultsHash(results, assetOperations)
          validators  = blockchain.lastBlockContractValidators - minerAddress
          validationProofs <- selectValidationProofs(tx.id(), validators, validationPolicy, resultsHash)
          _                <- checkAssetOperationsSupported(contractNativeTokenFeatureActivated, assetOperations)
          _                <- checkLeaseOpsForContractSupported(leaseOpsForContractsFeatureActivated, assetOperations)
          executedTx <-
            extractInputCommitment(tx)
              .filter(_ => confidentialContractFeatureActivated)
              .map { inputCommitment =>
                buildConfidentialExecutedTx(results, tx, resultsHash, validationProofs, inputCommitment)
              }.getOrElse {
                val executedTxOrError: Either[ValidationError, ExecutedContractTransaction] =
                  if (contractNativeTokenFeatureActivated) {
                    ExecutedContractTransactionV3.selfSigned(
                      nodeOwnerAccount,
                      tx,
                      results,
                      resultsHash,
                      validationProofs,
                      time.getTimestamp(),
                      assetOperations
                    )
                  } else {
                    ExecutedContractTransactionV2.selfSigned(nodeOwnerAccount, tx, results, resultsHash, validationProofs, time.getTimestamp())
                  }

                executedTxOrError.map { executedTx =>
                  ExecutedTxOutput(executedTx, None)
                }
              }

        } yield executedTx
      )
    } else {
      metrics.measureEither(
        CreateExecutedTx,
        ExecutedContractTransactionV1.selfSigned(nodeOwnerAccount, tx, results, time.getTimestamp())
          .map { executedTx =>
            ExecutedTxOutput(executedTx, None)
          }
      )
    }

  override def collectContractValidatorResults(keyBlockId: ByteStr,
                                               txId: ByteStr,
                                               validators: Set[Address],
                                               resultHash: Option[ByteStr],
                                               limit: Option[Int]): List[ContractValidatorResults] =
    contractValidatorResultsStore.findResults(keyBlockId, txId, validators, resultHash, limit).toList

  private def selectValidationProofs(txId: ByteStr,
                                     validators: Set[Address],
                                     validationPolicy: ValidationPolicy,
                                     resultsHash: ByteStr): Either[ValidationError, List[ValidationProof]] =
    super.selectContractValidatorResults(txId, validators, validationPolicy, resultsHash.some)
      .map(_.map(proof => ValidationProof(proof.sender, proof.signature)))

  override protected def handleUpdateSuccess(metrics: ContractExecutionMetrics,
                                             tx: ExecutableTransaction,
                                             maybeCertChainWithCrl: Option[(CertChain, CrlCollection)],
                                             atomically: Boolean): Either[ValidationError, TransactionWithDiff] = {

    metrics
      .measureEither(CreateExecutedTx, {
                       ExecutedContractTransactionV1.selfSigned(nodeOwnerAccount, tx, List.empty, time.getTimestamp())
                     })
      .leftMap { error =>
        handleExecutedTxCreationFailed(tx)(error)
        error
      }
      .flatMap { executedTx =>
        log.debug(s"Built executed transaction '${executedTx.id()}' for '${tx.id()}'")
        processExecutedTx(executedTx, metrics, maybeCertChainWithCrl, maybeConfidentialOutput = None, atomically)
      }
  }

  private def handleExecutedTxCreationFailed(tx: ExecutableTransaction): Function[ValidationError, Unit] = {
    case invalidProofsError: InvalidValidationProofs =>
      invalidProofsError.resultsHash.foreach {
        contractValidatorResultsStore.removeInvalidResults(keyBlockId, tx.id(), _)
      }
      log.warn(s"Suddenly not enough proofs for transaction '${tx.id()}'. $invalidProofsError")
    case error =>
      val message = s"Executed transaction creation error: '$error'"
      utx.remove(tx, Some(message), mustBeInPool = true)
      log.error(s"$message for tx '${tx.id()}'")
      messagesCache.put(tx.id(), ContractExecutionMessage(nodeOwnerAccount, tx.id(), Failure, None, message, time.correctedTime()))
  }

  private def processConfidentialOutput(output: ConfidentialOutput): Unit = {
    confidentialStorage.saveOutput(output)
    val inventory = ConfidentialInventory(nodeOwnerAccount, output.contractId, output.commitment, ConfidentialDataType.Output)
    broadcastInventory(inventory)
  }

  private def processExecutedTx(
      executedTx: ExecutedContractTransaction,
      metrics: ContractExecutionMetrics,
      maybeCertChainWithCrl: Option[(CertChain, CrlCollection)],
      maybeConfidentialOutput: Option[ConfidentialOutput],
      atomically: Boolean
  ): Either[ValidationError, TransactionWithDiff] = {
    metrics
      .measureEither(
        ProcessContractTx,
        if (atomically) {
          transactionsAccumulator.processAtomically(executedTx, maybeConfidentialOutput, maybeCertChainWithCrl)
        } else {
          transactionsAccumulator.process(executedTx, maybeConfidentialOutput, maybeCertChainWithCrl)
        }
      )
      .map { diff =>
        txMetrics.put(executedTx.id(), metrics)
        TransactionWithDiff(executedTx, diff)
      }
      .leftMap {
        case constraintsOverflowError: ConstraintsOverflowError =>
          log.debug(s"Executed tx '${executedTx.id()}' for '${executedTx.tx.id()}' was discarded because it exceeds the constraints")
          constraintsOverflowError
        case MvccConflictError =>
          log.debug(s"Executed tx '${executedTx.id()}' for '${executedTx.tx.id()}' was discarded because it caused MVCC conflict")
          mvccConflictCounter.increment()
          metrics.markMvccConflict()
          MvccConflictError
        case error =>
          val tx = executedTx.tx
          utx.removeAll(Map[Transaction, String](tx -> error.toString))
          val message = s"Can't process executed transaction '${executedTx.id()}', error '$error'"
          log.error(message)
          messagesCache.put(tx.id(), ContractExecutionMessage(nodeOwnerAccount, tx.id(), Failure, None, message, time.correctedTime()))
          error
      }
  }
}
