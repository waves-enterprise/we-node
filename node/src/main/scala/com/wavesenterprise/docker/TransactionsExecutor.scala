package com.wavesenterprise.docker

import cats.data.{EitherT, OptionT}
import cats.implicits._
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.crypto.internals.confidentialcontracts.Commitment
import com.wavesenterprise.database.rocksdb.confidential.ConfidentialRocksDBStorage
import com.wavesenterprise.docker.ContractExecutionError.{FatalErrorCode, NodeFailure, RecoverableErrorCode}
import com.wavesenterprise.docker.ContractExecutionStatus.{Error, Failure}
import com.wavesenterprise.docker.StoredContract.{DockerContract, WasmContract}
import com.wavesenterprise.docker.TxContext.{AtomicInner, Default, TxContext}
import com.wavesenterprise.docker.exceptions.FatalExceptionsMatchers._
import com.wavesenterprise.docker.grpc.GrpcDockerContractExecutor
import com.wavesenterprise.docker.grpc.service.ContractReadLogService
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.metrics.docker.ContractExecutionMetrics
import com.wavesenterprise.mining._
import com.wavesenterprise.network.contracts.ConfidentialDataUtils
import com.wavesenterprise.state.contracts.confidential.{ConfidentialInput, ConfidentialOutput}
import com.wavesenterprise.state.diffs.AssetTransactionsDiff.checkAssetIdLength
import com.wavesenterprise.state.{Blockchain, ByteStr, ContractId, DataEntry, NG}
import com.wavesenterprise.transaction.ValidationError.{ContractNotFound, InvalidValidationProofs, MvccConflictError}
import com.wavesenterprise.transaction.docker.ContractTransactionEntryOps.DataEntryMap
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation.{
  ContractAssetOperationMap,
  ContractBurnV1,
  ContractCancelLeaseV1,
  ContractIssueV1,
  ContractLeaseV1,
  ContractPaymentV1,
  ContractReissueV1,
  ContractTransferOutV1
}
import com.wavesenterprise.transaction.wasm.WasmContractSupported
import com.wavesenterprise.transaction.{AtomicTransaction, StoredContractSupported, Transaction, ValidationError}
import com.wavesenterprise.utils.pki.CrlCollection
import com.wavesenterprise.utils.{ScorexLogging, Time}
import com.wavesenterprise.utx.UtxPool
import com.wavesenterprise.wasm.WASMContractExecutor
import com.wavesenterprise.{ContractExecutor, getDockerContract}
import kamon.Kamon
import kamon.metric.CounterMetric
import monix.eval.{Coeval, Task}
import monix.execution.Scheduler

import scala.util.control.NonFatal

trait TransactionsExecutor extends ScorexLogging {

  import TransactionsExecutor._

  protected val mvccConflictCounter: CounterMetric = Kamon.counter("mvcc-conflict-counter")

  def utx: UtxPool
  def blockchain: Blockchain with NG
  def transactionsAccumulator: TransactionsAccumulator
  def messagesCache: ContractExecutionMessagesCache
  def nodeOwnerAccount: PrivateKeyAccount
  def time: Time
  def grpcContractExecutor: GrpcDockerContractExecutor
  def wasmContractExecutor: WASMContractExecutor
  def keyBlockId: ByteStr
  def confidentialStorage: ConfidentialRocksDBStorage
  def readLogService: ContractReadLogService

  implicit def scheduler: Scheduler

  def parallelism: Int

  def prepareSetup(tx: ExecutableTransaction, maybeCertChainWithCrl: Option[(CertChain, CrlCollection)]): Task[DefaultExecutableTxSetup] = {
    for {
      info     <- deferEither(contractInfo(tx))
      executor <- deferEither(selectExecutor(tx))
    } yield DefaultExecutableTxSetup(tx, executor, info, parallelism, maybeCertChainWithCrl)
  }

  private def loadConfidentialInput(tx: CallContractTransactionV6): Either[ContractExecutionException, ConfidentialInput] = {
    tx.inputCommitment.fold(
      Either.left[ContractExecutionException, ConfidentialInput](
        ContractExecutionException(ValidationError.ContractExecutionError(tx.contractId, "input commitment not defined"))
      ))(c =>
      confidentialStorage.getInput(c).toRight {
        ContractExecutionException(
          ValidationError.ContractExecutionError(tx.contractId, s"Confidential input '$c' for tx '${tx.id()}' not found"))
      })
  }

  def prepareConfidentialSetup(tx: CallContractTransactionV6,
                               contractInfo: ContractInfo,
                               maybeCertChainWithCrl: Option[(CertChain, CrlCollection)]): Task[ConfidentialCallPermittedSetup] = {
    for {
      executor          <- deferEither(selectExecutor(tx))
      confidentialInput <- deferEither(loadConfidentialInput(tx))
    } yield ConfidentialCallPermittedSetup(tx, executor, contractInfo, confidentialInput, maybeCertChainWithCrl)
  }

  protected def extractInputCommitment(tx: ExecutableTransaction): Option[Commitment] =
    tx match {
      case tx: CallContractTransactionV6 if blockchain.contract(ContractId(tx.contractId)).exists(_.isConfidential) =>
        tx.inputCommitment
      case _ =>
        None
    }

  protected case class ExecutedTxOutput(tx: ExecutedContractTransaction, confidentialOutput: Seq[ConfidentialOutput])

  // noinspection UnstableApiUsage
  protected def buildConfidentialExecutedTx(results: DataEntryMap,
                                            tx: ExecutableTransaction,
                                            resultsHash: ByteStr,
                                            validationProofs: List[ValidationProof],
                                            inputCommitment: Commitment): Either[ValidationError, ExecutedTxOutput] = {
    val expectedHash = ContractTransactionValidation.resultsMapHash(results, ContractAssetOperationMap(Map.empty))
    Either.cond(
      resultsHash == expectedHash,
      (),
      ValidationError.InvalidResultsHash(resultsHash, expectedHash)
    ).flatMap(_ =>
      confidentialStorage.getInput(inputCommitment).toRight {
        ValidationError.GenericError(s"Confidential input for tx '${tx.id()}' and commitment '$inputCommitment' not found")
      }.flatMap {
        confidentialInput =>
          val data             = ConfidentialDataUtils.entryMapToBytes(results)
          val outputCommitment = Commitment.create(data, confidentialInput.commitmentKey)
          val confidentialOutputs = results.mapping.map {
            case (contractId, dataEntries) => ConfidentialOutput(
                commitment = outputCommitment,
                txId = tx.id(),
                contractId = ContractId(contractId),
                commitmentKey = confidentialInput.commitmentKey,
                entries = dataEntries
              )
          }

          val (readings, readingsHashOpt) = readLogService.createFinalReadingsJournal(tx.id())

          ExecutedContractTransactionV5.selfSigned(
            sender = nodeOwnerAccount,
            tx = tx,
            resultsMap = DataEntryMap(Map.empty), // results of confidential tx is not for public, it's encoded in outputCommitment
            resultsHash = resultsHash,
            validationProofs = validationProofs,
            timestamp = time.getTimestamp(),
            assetOperationsMap = ContractAssetOperationMap(Map.empty),
            statusCode = 0,
            errorMessage = None,
            readings = readings.toList,
            readingsHash = readingsHashOpt,
            outputCommitment = Some(outputCommitment),
          ).map { executedTx =>
            ExecutedTxOutput(executedTx, confidentialOutputs.toSeq)
          }
      })
  }

  // noinspection UnstableApiUsage
  protected def buildConfidentialExecutedTx(results: List[DataEntry[_]],
                                            tx: ExecutableTransaction,
                                            resultsHash: ByteStr,
                                            validationProofs: List[ValidationProof],
                                            inputCommitment: Commitment): Either[ValidationError, ExecutedTxOutput] = {
    val expectedHash = ContractTransactionValidation.resultsHash(results, List.empty)
    Either.cond(
      resultsHash == expectedHash,
      (),
      ValidationError.InvalidResultsHash(resultsHash, expectedHash)
    ).flatMap(_ =>
      confidentialStorage.getInput(inputCommitment).toRight {
        ValidationError.GenericError(s"Confidential input for tx '${tx.id()}' and commitment '$inputCommitment' not found")
      }.flatMap {
        confidentialInput =>
          val data             = ConfidentialDataUtils.entriesToBytes(results)
          val outputCommitment = Commitment.create(data, confidentialInput.commitmentKey)
          val confidentialOutput = ConfidentialOutput(
            commitment = outputCommitment,
            txId = tx.id(),
            contractId = ContractId(tx.contractId),
            commitmentKey = confidentialInput.commitmentKey,
            entries = results
          )

          val (readings, readingsHashOpt) = readLogService.createFinalReadingsJournal(tx.id())

          ExecutedContractTransactionV4.selfSigned(
            sender = nodeOwnerAccount,
            tx = tx,
            results = List.empty, // results of confidential tx is not for public, it's encoded in outputCommitment
            resultsHash = resultsHash,
            validationProofs = validationProofs,
            timestamp = time.getTimestamp(),
            assetOperations = List.empty,
            readings = readings.toList,
            readingsHash = readingsHashOpt,
            outputCommitment = Some(outputCommitment)
          ).map { executedTx =>
            ExecutedTxOutput(executedTx, Seq(confidentialOutput))
          }
      })
  }

  def contractReady(tx: ExecutableTransaction, onReady: Coeval[Unit]): Task[Boolean] = {
    for {
      info     <- deferEither(contractInfo(tx))
      executor <- deferEither(selectExecutor(tx))
      ready    <- checkContractReady(tx, info, executor, onReady, handleThrowable())
    } yield ready
  }

  /**
    * For Update transaction we check if image exists. If it doesn't exist we pull it from registry async-ly.
    * For Create / Call transactions we check if container has been started. If it hasn't we start it async-ly.
    */
  private def checkContractReady(tx: ExecutableTransaction,
                                 contract: ContractInfo,
                                 executor: ContractExecutor,
                                 onReady: Coeval[Unit],
                                 onFailure: (ExecutableTransaction, Throwable) => Unit): Task[Boolean] = Task.defer {
    if (contract.storedContract.isInstanceOf[WasmContract]) {
      Task.pure(true)
    } else {
      executor match {
        case _: WASMContractExecutor =>
          Task.raiseError(new RuntimeException(s"unexpected WASMContractExecutor for docker contract TX $tx"))
        case docker: DockerContractExecutor =>
          val onFailureCurried = onFailure.curried
          tx match {
            case update: UpdateContractTransaction => checkExistsOrPull(update, ContractInfo(update, contract), docker, onReady, onFailureCurried(tx))
            case createOrCall                      => checkStartedOrStart(createOrCall, contract, docker, onReady, onFailureCurried(tx))
          }
      }
    }
  }

  private def checkExistsOrPull(tx: UpdateContractTransaction,
                                contract: ContractInfo,
                                executor: DockerContractExecutor,
                                onReady: Coeval[Unit],
                                onFailure: Throwable => Unit): Task[Boolean] = {
    executor
      .contractExists(contract)
      .map { exists =>
        val DockerContract(img, _, _) = getDockerContract(contract)
        if (exists) {
          log.trace(s"Contract image '$img' exists")
        } else {
          log.trace(s"Contract image '$img' does not exist")
          EitherT(executor.inspectOrPullContract(contract, ContractExecutionMetrics(tx)).attempt)
            .bimap(onFailure, _ => onReady())
        }
        exists
      }
      .onErrorRecover {
        case NonFatal(exception) =>
          log.error("Can't check contract exists", exception)
          false
      }
  }

  private def checkStartedOrStart(tx: ExecutableTransaction,
                                  contract: ContractInfo,
                                  executor: DockerContractExecutor,
                                  onReady: Coeval[Unit],
                                  onFailure: Throwable => Unit): Task[Boolean] = {
    executor.contractStarted(contract).map { started =>
      if (started) {
        log.trace(s"Contract '${contract.contractId}' container has already been started")
      } else {
        log.trace(s"Contract '${contract.contractId}' container has not been started yet")
        EitherT(executor.startContract(contract, ContractExecutionMetrics(tx)).attempt)
          .bimap(onFailure, _ => onReady())
      }
      started
    }
  }

  def atomicContractsReady(atomic: AtomicTransaction, onReady: Coeval[Unit]): Task[Boolean] = Task.defer {
    accumulateExecutableContracts(atomic).flatMap {
      case ExecutableContractsAccumulator(_, txs) =>
        /* Transactions are prepended so we have to reverse them */
        txs.reverse.forallM {
          case (tx, contract) => checkContractReady(tx, contract, grpcContractExecutor, onReady, handleAtomicThrowable(atomic))
        }
    }
  }

  private def accumulateExecutableContracts(atomic: AtomicTransaction): Task[ExecutableContractsAccumulator] = {
    /* Atomic transaction can have complex chain of executable transactions like "create -> call -> update -> call".
       This code deals with them to find `contract info` for contracts start-up. Doesn't looks simple and beautiful but works.
       We go from first to last executable tx using foldLeft.
       If tx is CreateContract we add new contract to accumulator.
       If tx is CallContract first we try to find new contract from accumulator. If `contract info` not found in accumulator we try
       to find it from state.
       If tx is UpdateContract we update contract in accumulator.
     */

    def getContract(accumulator: ExecutableContractsAccumulator, tx: ExecutableTransaction): Task[ContractInfo] =
      OptionT.fromOption[Task](accumulator.contracts.get(tx.contractId)).getOrElseF(deferEither(contractInfo(tx)))

    atomic.transactions
      .collect {
        case tx: ExecutableTransaction => tx
      }
      .foldM(ExecutableContractsAccumulator.Empty) {
        case (accumulator, tx) =>
          tx match {
            case create: CreateContractTransaction => Task.pure(accumulator.withCreate(create))
            case call: CallContractTransaction     => getContract(accumulator, call).map(ci => accumulator.withCall(call, ci))
            case update: UpdateContractTransaction => getContract(accumulator, update).map(ci => accumulator.withUpdate(update, ci))
          }
      }
  }

  def contractInfo(tx: ExecutableTransaction): Either[ContractExecutionException, ContractInfo] = {
    tx match {
      case createTx: CreateContractTransaction => Right(ContractInfo(createTx))
      case _                                   => transactionsAccumulator.contract(ContractId(tx.contractId)).toRight(ContractExecutionException(ContractNotFound(tx.contractId)))
    }
  }

  private def selectExecutor(executableTransaction: ExecutableTransaction): Either[ContractExecutionException, ContractExecutor] = {
    executableTransaction match {
      case _: CreateContractTransactionV1 =>
        Left(ContractExecutionException(ValidationError.ContractExecutionError(
          executableTransaction.contractId,
          "CreateContractTransactionV1 support was deleted as deprecated")))
      case tx: StoredContractSupported if tx.storedContract.engine() == "wasm"   => Right(wasmContractExecutor)
      case tx: StoredContractSupported if tx.storedContract.engine() == "docker" => Right(grpcContractExecutor)
      case tx: WasmContractSupported if tx.contractEngine == "wasm"              => Right(wasmContractExecutor)
      case tx: WasmContractSupported if tx.contractEngine == "docker"            => Right(grpcContractExecutor)
      case _: CreateContractTransaction | _: UpdateContractTransaction           => Right(grpcContractExecutor)
      case _ =>
        for {
          executedTx <- transactionsAccumulator
            .executedTxFor(executableTransaction.contractId)
            .toRight(ContractExecutionException(ContractNotFound(executableTransaction.contractId)))
          executor <- selectExecutor(executedTx.tx)
        } yield executor
    }
  }

  def processSetup(setup: ExecutableSetup,
                   atomically: Boolean = false,
                   txContext: TxContext = TxContext.Default): Task[Either[ValidationError, TransactionWithDiff]] = {
    Task(log.debug(s"Start executing contract transaction '${setup.tx.id()}'")) *>
      executeContract(setup.tx, setup.executor, setup.info, extractConfidentialInput(setup))
        .flatMap {
          case (value, metrics) =>
            handleExecutionResult(value, metrics, setup.tx, setup.maybeCertChainWithCrl, atomically, txContext = txContext)
        }
        .doOnCancel {
          Task(log.debug(s"Contract transaction '${setup.tx.id()}' execution was cancelled"))
        }
        .onErrorHandle { throwable =>
          handleThrowable()(setup.tx, throwable)
          Left(ValidationError.ContractExecutionError(setup.tx.id(), s"Contract execution exception: $throwable"))
        }
  }

  private def extractConfidentialInput(setup: ExecutableSetup): Option[ConfidentialInput] =
    setup match {
      case confidentialCallSetup: ConfidentialCallPermittedSetup => Some(confidentialCallSetup.input)
      case _                                                     => None
    }

  private def executeContract(tx: ExecutableTransaction,
                              executor: ContractExecutor,
                              info: ContractInfo,
                              maybeConfidentialInput: Option[ConfidentialInput]): Task[(ContractExecution, ContractExecutionMetrics)] = {
    val metricsStore = metricsFor(executor)
    Task
      .defer {
        metricsStore.contractsExecutedStarted.increment()
        val metrics = ContractExecutionMetrics(info.contractId, tx.id(), tx.txType)
        executor.executeTransaction(info, tx, maybeConfidentialInput, metrics).map(_ -> metrics)
      }
      .executeOn(scheduler)
      .doOnFinish { errorOpt =>
        Task.eval(errorOpt.fold(metricsStore.contractsExecutedFinished)(_ => metricsStore.contractsExecutedFailed).increment())
      }
  }

  private def handleExecutionResult(
      execution: ContractExecution,
      metrics: ContractExecutionMetrics,
      transaction: ExecutableTransaction,
      maybeCertChainWithCrl: Option[(CertChain, CrlCollection)],
      atomically: Boolean,
      txContext: TxContext
  ): Task[Either[ValidationError, TransactionWithDiff]] =
    Task {
      execution match {
        case ContractExecutionSuccessV2(results, assetOperations) =>
          handleExecutionSuccess(
            DataEntryMap(results),
            ContractAssetOperationMap(assetOperations),
            metrics,
            transaction,
            maybeCertChainWithCrl,
            atomically
          ).left.flatMap {
            case MvccConflictError =>
              Either.left(MvccConflictError)
            case err: InvalidValidationProofs =>
              Either.left(err)
            case err =>
              handleExecutionError(
                1,
                err.toString,
                metrics,
                transaction,
                maybeCertChainWithCrl,
                atomically,
                txContext
              )
          }
        case ContractExecutionSuccess(results, assetOperations)
            if transaction.isInstanceOf[WasmContractSupported] || transaction.isInstanceOf[StoredContractSupported] =>
          handleExecutionSuccess(
            DataEntryMap(Map(transaction.contractId -> results)),
            ContractAssetOperationMap(Map(transaction.contractId -> assetOperations)),
            metrics,
            transaction,
            maybeCertChainWithCrl,
            atomically
          ).left.flatMap {
            case MvccConflictError =>
              Either.left(MvccConflictError)
            case err: InvalidValidationProofs =>
              Either.left(err)
            case err =>
              handleExecutionError(
                1,
                err.toString,
                metrics,
                transaction,
                maybeCertChainWithCrl,
                atomically,
                txContext)
          }
        case ContractExecutionSuccess(results, assetOperations) =>
          handleExecutionSuccess(results, assetOperations, metrics, transaction, maybeCertChainWithCrl, atomically)
        case ContractUpdateSuccess =>
          handleUpdateSuccess(metrics, transaction, maybeCertChainWithCrl, atomically)
        case ContractExecutionError(code, message) =>
          transaction match {
            case _: WasmContractSupported | _: StoredContractSupported =>
              handleExecutionError(
                if (code == FatalErrorCode) NodeFailure else code,
                message,
                metrics,
                transaction,
                maybeCertChainWithCrl,
                atomically,
                txContext
              )
            case _ =>
              handleError(code, message, transaction, txContext = txContext)
              if ((code == FatalErrorCode || code == NodeFailure) && txContext == AtomicInner)
                Left(ValidationError.GenericError(s"Contract execution error '$message' with code '$code' for transaction '${transaction.id()}'"))
              else
                Left(ValidationError.ContractExecutionError(transaction.id(), message))
          }
      }
    }

  private def handleAtomicThrowable(atomic: AtomicTransaction)(tx: ExecutableTransaction, t: Throwable): Unit = {
    log.error(s"Contract transaction '${tx.id()}' from atomic '${atomic.id()}' failed")
    handleThrowable(Some(atomic))(tx, t)
  }

  private def handleThrowable(maybePrimaryTx: Option[AtomicTransaction] = None)(tx: ExecutableTransaction, error: Throwable): Unit = {
    val stringTx = s"${tx.id()}${maybePrimaryTx.fold("")(primaryTx => s" (is inner tx of ${primaryTx.id()})")}"

    error match {
      case err if allFatalExceptionsMatcher(err) =>
        val stringError = fatalTxExceptionToString(err, stringTx)
        log.error(s"$stringError, drop it from UTX", err)
        utx.removeAll(Map[Transaction, String](maybePrimaryTx.getOrElse(tx) -> stringError))
      case NonFatal(err) =>
        log.error(s"Contract execution error: $err for transaction '$stringTx'", err)
    }

    val codeOpt = error match {
      case contractEx: ContractExecutionException => contractEx.code
      case _                                      => None
    }

    val message         = Option(error.getMessage).getOrElse(error.toString)
    val enrichedMessage = enrichStatusMessage(message)

    messagesCache.put(tx.id(), ContractExecutionMessage(nodeOwnerAccount, tx.id(), Failure, codeOpt, enrichedMessage, time.correctedTime()))
  }

  protected def enrichStatusMessage(message: String): String = message

  protected def handleError(code: Int, message: String, tx: ExecutableTransaction, txContext: TxContext): Unit = {
    val debugMessage = s"Contract execution error '$message' with code '$code' for transaction '${tx.id()}'"
    txContext match {
      case Default =>
        if (code == FatalErrorCode || code == NodeFailure) {
          log.debug(s"$debugMessage, drop it from UTX")
          utx.removeAll(Map[Transaction, String](tx -> s"Contract execution error '$message' with code '$code'"))
        }
      case AtomicInner =>
        log.debug(debugMessage)
    }
    val enrichedMessage = enrichStatusMessage(message)
    messagesCache.put(
      tx.id(),
      ContractExecutionMessage(nodeOwnerAccount,
                               tx.id(),
                               if (code == RecoverableErrorCode) Failure else Error,
                               Some(code),
                               enrichedMessage,
                               time.correctedTime())
    )
  }

  protected def handleUpdateSuccess(metrics: ContractExecutionMetrics,
                                    tx: ExecutableTransaction,
                                    maybeCertChainWithCrl: Option[(CertChain, CrlCollection)],
                                    atomically: Boolean): Either[ValidationError, TransactionWithDiff]

  protected def handleExecutionSuccess(
      results: List[DataEntry[_]],
      assetOperations: List[ContractAssetOperation],
      metrics: ContractExecutionMetrics,
      tx: ExecutableTransaction,
      maybeCertChainWithCrl: Option[(CertChain, CrlCollection)],
      atomically: Boolean
  ): Either[ValidationError, TransactionWithDiff]

  protected def handleExecutionSuccess(
      results: DataEntryMap,
      assetOperations: ContractAssetOperationMap,
      metrics: ContractExecutionMetrics,
      tx: ExecutableTransaction,
      maybeCertChainWithCrl: Option[(CertChain, CrlCollection)],
      atomically: Boolean
  ): Either[ValidationError, TransactionWithDiff]

  protected def handleExecutionError(
      statusCode: Int,
      errorMessage: String,
      metrics: ContractExecutionMetrics,
      tx: ExecutableTransaction,
      maybeCertChainWithCrl: Option[(CertChain, CrlCollection)],
      atomically: Boolean,
      txContext: TxContext
  ): Either[ValidationError, TransactionWithDiff]

  def checkAssetOperationsSupported(
      contractNativeTokenFeatureActivated: Boolean,
      assetOperations: List[ContractAssetOperation]
  ): Either[ValidationError, Unit] =
    Either.cond(contractNativeTokenFeatureActivated || assetOperations.isEmpty, (), ValidationError.BaseAssetOpsNotSupported)

  def checkLeaseOpsForContractSupported(
      leaseOpsForContractsFeatureActivated: Boolean,
      assetOperation: List[ContractAssetOperation]
  ): Either[ValidationError, Unit] = {
    val containsLeaseOps = assetOperation.exists {
      case _: ContractLeaseV1 | _: ContractCancelLeaseV1 => true
      case _                                             => false
    }

    Either.cond(leaseOpsForContractsFeatureActivated || !containsLeaseOps, (), ValidationError.LeaseAssetOpsNotSupported)
  }

  def deriveValidationPolicy(contractId: ByteStr, policies: Seq[ValidationPolicy]): Either[ValidationError, ValidationPolicy] = {
    case object IncompatiblePoliciesException extends Exception
    try {
      val result = policies.foldLeft(ValidationPolicy.Any: ValidationPolicy) { case (acc, policy) =>
        (acc, policy) match {
          case (_, ValidationPolicy.Any)                                               => acc
          case (ValidationPolicy.Any, _)                                               => policy
          case (ValidationPolicy.Majority, ValidationPolicy.Majority)                  => ValidationPolicy.Majority
          case (strict: ValidationPolicy.MajorityWithOneOf, ValidationPolicy.Majority) => strict
          case (ValidationPolicy.Majority, strict: ValidationPolicy.MajorityWithOneOf) => strict
          case (first: ValidationPolicy.MajorityWithOneOf, second: ValidationPolicy.MajorityWithOneOf) =>
            val intersect = first.addresses.toSet.intersect(second.addresses.toSet)
            if (intersect.isEmpty) {
              throw IncompatiblePoliciesException
            } else {
              ValidationPolicy.MajorityWithOneOf(intersect.toList)
            }
        }
      }
      Right(result)
    } catch {
      case IncompatiblePoliciesException => Left(ValidationError.ContractExecutionError(
          contractId,
          s"Incompatible policies found in contracts sub-calls: ${policies.mkString("[", ",", "]")}"
        ))
    }
  }

  def validateAssetIdLength(assetOperations: List[ContractAssetOperation]): Either[ValidationError, Unit] =
    assetOperations.traverse {
      case op: ContractIssueV1                           => checkAssetIdLength(op.assetId)
      case op: ContractReissueV1                         => checkAssetIdLength(op.assetId)
      case op: ContractTransferOutV1                     => op.assetId.fold[Either[ValidationError, Unit]](Right(()))(checkAssetIdLength)
      case op: ContractBurnV1                            => op.assetId.fold[Either[ValidationError, Unit]](Right(()))(checkAssetIdLength)
      case op: ContractPaymentV1                         => op.assetId.fold[Either[ValidationError, Unit]](Right(()))(checkAssetIdLength)
      case _: ContractLeaseV1 | _: ContractCancelLeaseV1 => Right(())
    }.void
}

object TransactionsExecutor {

  trait ContractMetrics {
    val contractsExecutedStarted: CounterMetric
    val contractsExecutedFinished: CounterMetric
    val contractsExecutedFailed: CounterMetric
  }

  object DockerContractMetrics extends ContractMetrics {
    val contractsExecutedStarted: CounterMetric  = Kamon.counter("docker-contracts-started")
    val contractsExecutedFinished: CounterMetric = Kamon.counter("docker-contracts-finished")
    val contractsExecutedFailed: CounterMetric   = Kamon.counter("docker-contracts-failed")
  }

  object WASMContractMetrics extends ContractMetrics {
    val contractsExecutedStarted: CounterMetric  = Kamon.counter("wasm-contracts-started")
    val contractsExecutedFinished: CounterMetric = Kamon.counter("wasm-contracts-finished")
    val contractsExecutedFailed: CounterMetric   = Kamon.counter("wasm-contracts-failed")
  }

  def metricsFor(executor: ContractExecutor) = executor match {
    case _: DockerContractExecutor => DockerContractMetrics
    case _: WASMContractExecutor   => WASMContractMetrics
  }
}

private[docker] case class ExecutableContractsAccumulator(contracts: Map[ByteStr, ContractInfo], txs: List[(ExecutableTransaction, ContractInfo)]) {

  def withCreate(create: CreateContractTransaction): ExecutableContractsAccumulator = {
    val contract = ContractInfo(create)
    val updated  = contracts + (create.contractId -> ContractInfo(create))
    ExecutableContractsAccumulator(updated, (create -> contract) :: txs)
  }

  def withCall(call: CallContractTransaction, contract: ContractInfo): ExecutableContractsAccumulator = {
    copy(txs = (call -> contract) :: txs)
  }

  def withUpdate(update: UpdateContractTransaction, contract: ContractInfo): ExecutableContractsAccumulator = {
    val updatedContract = ContractInfo(update, contract)
    ExecutableContractsAccumulator(contracts = contracts + (update.contractId -> updatedContract), txs = (update -> updatedContract) :: txs)
  }
}

object ExecutableContractsAccumulator {
  private[docker] val Empty = ExecutableContractsAccumulator(Map.empty, List.empty)

}

object TxContext {
  sealed trait TxContext

  case object Default extends TxContext

  case object AtomicInner extends TxContext
}
