package com.wavesenterprise.docker

import cats.implicits._
import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.block.{MicroBlock, TxMicroBlock}
import com.wavesenterprise.docker.grpc.GrpcContractExecutor
import com.wavesenterprise.docker.validator.{ContractValidatorResultsStore, ValidationPolicy}
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.features.FeatureProvider.FeatureProviderExt
import com.wavesenterprise.metrics.docker.{ContractExecutionMetrics, CreateExecutedTx, ProcessContractTx}
import com.wavesenterprise.mining.{TransactionWithDiff, TransactionsAccumulator}
import com.wavesenterprise.network.ContractValidatorResults
import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.state.{Blockchain, ByteStr, DataEntry, NG}
import com.wavesenterprise.transaction.ValidationError.{ConstraintsOverflowError, InvalidValidationProofs, MvccConflictError}
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation
import com.wavesenterprise.transaction.{AtomicTransaction, Transaction, ValidationError}
import com.wavesenterprise.utils.Time
import com.wavesenterprise.utils.pki.CrlCollection
import com.wavesenterprise.utx.UtxPool
import monix.execution.Scheduler

import java.util.concurrent.ConcurrentHashMap

class MinerTransactionsExecutor(
    val messagesCache: ContractExecutionMessagesCache,
    val transactionsAccumulator: TransactionsAccumulator,
    val nodeOwnerAccount: PrivateKeyAccount,
    val utx: UtxPool,
    val blockchain: Blockchain with NG,
    val time: Time,
    val legacyContractExecutor: LegacyContractExecutor,
    val grpcContractExecutor: GrpcContractExecutor,
    val contractValidatorResultsStore: ContractValidatorResultsStore,
    val keyBlockId: ByteStr,
    val parallelism: Int
)(implicit val scheduler: Scheduler)
    extends TransactionsExecutor {

  import ContractExecutionStatus._

  private[this] val minerAddress = nodeOwnerAccount.toAddress

  private[this] val txMetrics = new ConcurrentHashMap[ByteStr, ContractExecutionMetrics]()
  private[this] val validationFeatureActivated: Boolean =
    blockchain.isFeatureActivated(BlockchainFeature.ContractValidationsSupport, blockchain.height)
  private[this] val contractNativeTokenFeatureActivated: Boolean =
    blockchain.isFeatureActivated(BlockchainFeature.ContractNativeTokenSupportAndPkiV1Support, blockchain.height)

  contractValidatorResultsStore.removeExceptFor(keyBlockId)

  def selectExecutableTxPredicate(tx: ExecutableTransaction, accumulatedValidationPolicies: Map[ByteStr, ValidationPolicy] = Map.empty): Boolean =
    !validationFeatureActivated || enoughProofs(tx, accumulatedValidationPolicies)

  private def enoughProofs(tx: ExecutableTransaction, accumulatedValidationPolicies: Map[ByteStr, ValidationPolicy]): Boolean = {
    (accumulatedValidationPolicies.get(tx.contractId) orElse transactionsAccumulator.validationPolicy(tx).toOption)
      .exists {
        case ValidationPolicy.Any                          => true
        case ValidationPolicy.Majority                     => checkProofsMajority(tx.id())
        case ValidationPolicy.MajorityWithOneOf(addresses) => checkProofsMajority(tx.id(), addresses.toSet)
      }
  }

  private def checkProofsMajority(txId: ByteStr, requiredAddresses: Set[Address] = Set.empty): Boolean = {
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
    createExecutedTx(results, assetOperations, metrics, tx)
      .leftMap { error =>
        handleExecutedTxCreationFailed(tx)(error)
        error
      }
      .flatMap { executedTx =>
        log.debug(s"Built executed transaction '${executedTx.id()}' for '${tx.id()}'")
        processExecutedTx(executedTx, metrics, maybeCertChainWithCrl, atomically)
      }

  private def createExecutedTx(
      results: List[DataEntry[_]],
      assetOperations: List[ContractAssetOperation],
      metrics: ContractExecutionMetrics,
      tx: ExecutableTransaction
  ): Either[ValidationError, ExecutedContractTransaction] =
    if (validationFeatureActivated) {
      metrics.measureEither(
        CreateExecutedTx,
        for {
          validationPolicy <- transactionsAccumulator.validationPolicy(tx)
          resultsHash = ContractTransactionValidation.resultsHash(results, assetOperations)
          validators  = blockchain.lastBlockContractValidators - minerAddress
          validationProofs <- selectValidationProofs(tx.id(), validators, validationPolicy, resultsHash)
          _                <- checkAssetOperationsAreSupported(contractNativeTokenFeatureActivated, assetOperations)
          executedTx <- if (contractNativeTokenFeatureActivated) {
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
        } yield executedTx
      )
    } else {
      metrics.measureEither(
        CreateExecutedTx,
        ExecutedContractTransactionV1.selfSigned(nodeOwnerAccount, tx, results, time.getTimestamp())
      )
    }

  private def selectValidationProofs(txId: ByteStr,
                                     validators: Set[Address],
                                     validationPolicy: ValidationPolicy,
                                     resultsHash: ByteStr): Either[ValidationError, List[ValidationProof]] =
    validationPolicy match {
      case ValidationPolicy.Any => Right(List.empty)
      case ValidationPolicy.Majority =>
        val majoritySize = math.ceil(validators.size * ValidationPolicy.MajorityRatio).toInt
        val result       = contractValidatorResultsStore.findResults(keyBlockId, txId, validators, Some(resultsHash), Some(majoritySize))

        Either.cond(
          result.size >= majoritySize,
          result.view.map(proof => ValidationProof(proof.sender, proof.signature)).toList,
          InvalidValidationProofs(result.size, majoritySize, validators, resultsHash)
        )
      case ValidationPolicy.MajorityWithOneOf(addresses) =>
        val requiredAddresses = addresses.toSet
        val majoritySize      = math.ceil(validators.size * ValidationPolicy.MajorityRatio).toInt
        val validatorResults  = contractValidatorResultsStore.findResults(keyBlockId, txId, validators, Some(resultsHash))
        val (results, resultsSize, resultsContainsRequiredAddresses) = validatorResults
          .foldLeft((List.empty[ContractValidatorResults], 0, false)) {
            case ((acc, accSize, accContainsRequired), i) =>
              if (accContainsRequired && accSize >= majoritySize)
                (acc, accSize, accContainsRequired)
              else if (!accContainsRequired && accSize >= majoritySize && requiredAddresses.contains(i.sender.toAddress))
                (i :: acc.tail, accSize, true)
              else
                (i :: acc, accSize + 1, accContainsRequired || requiredAddresses.contains(i.sender.toAddress))
          }

        Either.cond(
          resultsContainsRequiredAddresses && resultsSize >= majoritySize,
          results.reverseMap(proof => ValidationProof(proof.sender, proof.signature)),
          InvalidValidationProofs(resultsSize, majoritySize, validators, resultsHash, resultsContainsRequiredAddresses, requiredAddresses)
        )
    }

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
        processExecutedTx(executedTx, metrics, maybeCertChainWithCrl, atomically)
      }
  }

  private def handleExecutedTxCreationFailed(tx: ExecutableTransaction): Function[ValidationError, Unit] = {
    case invalidProofsError: InvalidValidationProofs =>
      contractValidatorResultsStore.removeInvalidResults(keyBlockId, tx.id(), invalidProofsError.resultsHash)
      log.warn(s"Suddenly not enough proofs for transaction '${tx.id()}'. $invalidProofsError")
    case error =>
      val message = s"Executed transaction creation error: '$error'"
      utx.remove(tx, Some(message), mustBeInPool = true)
      log.error(s"$message for tx '${tx.id()}'")
      messagesCache.put(tx.id(), ContractExecutionMessage(nodeOwnerAccount, tx.id(), Failure, None, message, time.correctedTime()))
  }

  private def processExecutedTx(
      executedTx: ExecutedContractTransaction,
      metrics: ContractExecutionMetrics,
      maybeCertChainWithCrl: Option[(CertChain, CrlCollection)],
      atomically: Boolean
  ): Either[ValidationError, TransactionWithDiff] = {
    metrics
      .measureEither(
        ProcessContractTx,
        if (atomically) {
          transactionsAccumulator.processAtomically(executedTx, maybeCertChainWithCrl)
        } else {
          transactionsAccumulator.process(executedTx, maybeCertChainWithCrl)
        }
      )
      .map { diff =>
        txMetrics.put(executedTx.id(), metrics)
        TransactionWithDiff(executedTx, diff)
      }
      .leftMap {
        case ConstraintsOverflowError =>
          log.debug(s"Executed tx '${executedTx.id()}' for '${executedTx.tx.id()}' was discarded because it exceeds the constraints")
          ConstraintsOverflowError
        case MvccConflictError =>
          log.debug(s"Executed tx '${executedTx.id()}' for '${executedTx.tx.id()}' was discarded because it caused MVCC conflict")
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
