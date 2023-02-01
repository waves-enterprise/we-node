package com.wavesenterprise.docker

import cats.data.{EitherT, OptionT}
import cats.implicits._
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.docker.ContractExecutionStatus.{Error, Failure}
import com.wavesenterprise.docker.TxContext.{AtomicInner, Default, TxContext}
import com.wavesenterprise.docker.exceptions.FatalExceptionsMatchers._
import com.wavesenterprise.docker.grpc.GrpcContractExecutor
import com.wavesenterprise.metrics.docker.ContractExecutionMetrics
import com.wavesenterprise.mining.{ExecutableTxSetup, TransactionWithDiff, TransactionsAccumulator}
import com.wavesenterprise.state.{Blockchain, ByteStr, ContractId, DataEntry, NG}
import com.wavesenterprise.state.diffs.AssetTransactionsDiff.checkAssetIdLength
import com.wavesenterprise.transaction.ValidationError.ContractNotFound
import com.wavesenterprise.transaction.docker._
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation
import com.wavesenterprise.transaction.docker.assets.ContractAssetOperation.{
  ContractBurnV1,
  ContractIssueV1,
  ContractReissueV1,
  ContractTransferOutV1
}
import com.wavesenterprise.transaction.{AtomicTransaction, Transaction, ValidationError}
import com.wavesenterprise.utils.pki.CrlCollection
import com.wavesenterprise.utils.{ScorexLogging, Time}
import com.wavesenterprise.utx.UtxPool
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
  def grpcContractExecutor: GrpcContractExecutor
  def keyBlockId: ByteStr

  implicit def scheduler: Scheduler

  def parallelism: Int

  def prepareSetup(tx: ExecutableTransaction, maybeCertChainWithCrl: Option[(CertChain, CrlCollection)]): Task[ExecutableTxSetup] = {
    for {
      info     <- deferEither(contractInfo(tx))
      executor <- deferEither(selectExecutor(tx))
    } yield ExecutableTxSetup(tx, executor, info, parallelism, maybeCertChainWithCrl)
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
    val onFailureCurried = onFailure.curried
    tx match {
      case update: UpdateContractTransaction => checkExistsOrPull(update, ContractInfo(update, contract), executor, onReady, onFailureCurried(tx))
      case createOrCall                      => checkStartedOrStart(createOrCall, contract, executor, onReady, onFailureCurried(tx))
    }
  }

  private def checkExistsOrPull(tx: UpdateContractTransaction,
                                contract: ContractInfo,
                                executor: ContractExecutor,
                                onReady: Coeval[Unit],
                                onFailure: Throwable => Unit): Task[Boolean] = {
    executor
      .contractExists(contract)
      .map { exists =>
        if (!exists) {
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
                                  executor: ContractExecutor,
                                  onReady: Coeval[Unit],
                                  onFailure: Throwable => Unit): Task[Boolean] = {
    executor.contractStarted(contract).map { started =>
      if (!started) {
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

  private def contractInfo(tx: ExecutableTransaction): Either[ContractExecutionException, ContractInfo] = {
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
      case _: CreateContractTransaction => Right(grpcContractExecutor)
      case _ =>
        for {
          executedTx <- transactionsAccumulator
            .executedTxFor(executableTransaction.contractId)
            .toRight(ContractExecutionException(ContractNotFound(executableTransaction.contractId)))
          executor <- selectExecutor(executedTx.tx)
        } yield executor
    }
  }

  def processSetup(setup: ExecutableTxSetup,
                   atomically: Boolean = false,
                   txContext: TxContext = TxContext.Default): Task[Either[ValidationError, TransactionWithDiff]] = {
    Task(log.debug(s"Start executing contract transaction '${setup.tx.id()}'")) *>
      executeDockerContract(setup.tx, setup.executor, setup.info)
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

  protected def executeDockerContract(tx: ExecutableTransaction,
                                      executor: ContractExecutor,
                                      info: ContractInfo): Task[(ContractExecution, ContractExecutionMetrics)] =
    Task
      .defer {
        dockerContractsExecutedStarted.increment()
        val metrics = ContractExecutionMetrics(info.contractId, tx.id(), tx.txType)
        executor.executeTransaction(info, tx, metrics).map(_ -> metrics)
      }
      .executeOn(scheduler)
      .doOnFinish { errorOpt =>
        Task.eval(errorOpt.fold(dockerContractsExecutedFinished)(_ => dockerContractsExecutedFailed).increment())
      }

  protected def handleExecutionResult(
      execution: ContractExecution,
      metrics: ContractExecutionMetrics,
      transaction: ExecutableTransaction,
      maybeCertChainWithCrl: Option[(CertChain, CrlCollection)],
      atomically: Boolean,
      txContext: TxContext = TxContext.Default
  ): Task[Either[ValidationError, TransactionWithDiff]] =
    Task {
      execution match {
        case ContractExecutionSuccess(results, assetOperations) =>
          handleExecutionSuccess(results, assetOperations, metrics, transaction, maybeCertChainWithCrl, atomically)
        case ContractUpdateSuccess =>
          handleUpdateSuccess(metrics, transaction, maybeCertChainWithCrl, atomically)
        case ContractExecutionError(code, message) =>
          handleError(code, message, transaction, txContext = txContext)
          Left(ValidationError.ContractExecutionError(transaction.id(), message))
      }
    }

  private def handleAtomicThrowable(atomic: AtomicTransaction)(tx: ExecutableTransaction, t: Throwable): Unit = {
    log.error(s"Contract transaction '${tx.id()}' from atomic '${atomic.id()}' failed")
    handleThrowable(Some(atomic))(tx, t)
  }

  protected def handleThrowable(maybePrimaryTx: Option[AtomicTransaction] = None)(tx: ExecutableTransaction, error: Throwable): Unit = {
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

  protected def handleError(code: Int, message: String, tx: ExecutableTransaction, txContext: TxContext = TxContext.Default): Unit = {
    val debugMessage = s"Contract execution error '$message' with code '$code' for transaction '${tx.id()}'"
    txContext match {
      case Default =>
        log.debug(s"$debugMessage, drop it from UTX")
        utx.removeAll(Map[Transaction, String](tx -> s"Contract execution error '$message' with code '$code'"))
      case AtomicInner =>
        log.debug(debugMessage)
    }
    val enrichedMessage = enrichStatusMessage(message)
    messagesCache.put(tx.id(), ContractExecutionMessage(nodeOwnerAccount, tx.id(), Error, Some(code), enrichedMessage, time.correctedTime()))
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

  def checkAssetOperationsAreSupported(
      contractNativeTokenFeatureActivated: Boolean,
      assetOperations: List[ContractAssetOperation]
  ): Either[ValidationError, Unit] =
    Either.cond(contractNativeTokenFeatureActivated || assetOperations.isEmpty, (), ValidationError.UnsupportedAssetOperations)

  def validateAssetIdLength(assetOperations: List[ContractAssetOperation]): Either[ValidationError, Unit] =
    assetOperations.traverse {
      case op: ContractIssueV1       => checkAssetIdLength(op.assetId)
      case op: ContractReissueV1     => checkAssetIdLength(op.assetId)
      case op: ContractTransferOutV1 => op.assetId.fold[Either[ValidationError, Unit]](Right(()))(checkAssetIdLength)
      case op: ContractBurnV1        => op.assetId.fold[Either[ValidationError, Unit]](Right(()))(checkAssetIdLength)
    }.void
}

object TransactionsExecutor {
  val dockerContractsExecutedStarted: CounterMetric  = Kamon.counter("docker-contracts-started")
  val dockerContractsExecutedFinished: CounterMetric = Kamon.counter("docker-contracts-finished")
  val dockerContractsExecutedFailed: CounterMetric   = Kamon.counter("docker-contracts-failed")
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
