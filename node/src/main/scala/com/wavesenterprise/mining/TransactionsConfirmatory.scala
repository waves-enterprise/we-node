package com.wavesenterprise.mining

import cats.data.{EitherT, OptionT}
import cats.implicits._
import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.docker.validator.{ContractValidatorResultsStore, ValidationPolicy}
import com.wavesenterprise.docker.{ContractInfo, MinerTransactionsExecutor, TransactionsExecutor, TxContext, ValidatorTransactionsExecutor}
import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.database.rocksdb.confidential.ConfidentialRocksDBStorage
import com.wavesenterprise.settings.PositiveInt
import com.wavesenterprise.state.contracts.confidential.ConfidentialStateUpdater
import com.wavesenterprise.state.{Blockchain, ByteStr, ContractId, Diff}
import com.wavesenterprise.transaction.ValidationError.{
  ConstraintsOverflowError,
  CriticalConstraintOverflowError,
  GenericError,
  InvalidValidationProofs,
  MvccConflictError,
  OneConstraintOverflowError
}
import com.wavesenterprise.transaction.{ValidationError, _}
import com.wavesenterprise.transaction.docker.{
  CallContractTransactionV6,
  ConfidentialDataInCallContractSupported,
  CreateContractTransaction,
  ExecutableTransaction,
  ExecutedContractTransaction,
  ExecutedContractTransactionV5
}
import com.wavesenterprise.utils.{ScorexLogging, Time}
import com.wavesenterprise.utils.pki.CrlCollection
import com.wavesenterprise.utx.UtxPool
import com.wavesenterprise.utx.UtxPool.TxWithCerts
import kamon.Kamon
import kamon.metric.CounterMetric
import monix.catnap.{ConcurrentQueue, Semaphore}
import monix.eval.{Coeval, Task}
import monix.execution.atomic.AtomicInt
import monix.execution.{BufferCapacity, ChannelType, Scheduler}
import monix.reactive.observables.GroupedObservable
import monix.reactive.{Observable, OverflowStrategy}

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration._
import scala.util.control.NoStackTrace
import scala.util.{Left, Right}

/**
  * Processes transactions from UTX pool and provides the stream of confirmed transactions.
  */
trait TransactionsConfirmatory[E <: TransactionsExecutor] extends ScorexLogging {

  import TransactionsConfirmatory.Error._

  def transactionExecutorOpt: Option[E]

  protected def transactionsAccumulator: TransactionsAccumulator
  protected def utx: UtxPool
  protected def pullingBufferSize: PositiveInt
  protected def utxCheckDelay: FiniteDuration
  protected def ownerKey: PrivateKeyAccount

  implicit protected def scheduler: Scheduler

  protected val processedTxCounter: CounterMetric = Kamon.counter("tx-confirmatory.processed")
  protected val confirmedTxCounter: CounterMetric = Kamon.counter("tx-confirmatory.confirmed")

  private[this] val inProcessTxIds = ConcurrentHashMap.newKeySet[ByteStr]()
  private[this] val confirmedTxsQueue = ConcurrentQueue[Task].unsafe[TransactionWithDiff](
    capacity = BufferCapacity.Bounded(Runtime.getRuntime.availableProcessors() * 3),
    channelType = ChannelType.MPSC
  )

  private[mining] def confirmedTxsStream: Observable[TransactionWithDiff] = Observable.repeatEvalF(confirmedTxsQueue.poll)

  protected def selectTxPredicate(transaction: Transaction): Boolean = {
    !inProcessTxIds.contains(transaction.id())
  }

  def confirmationTask: Task[Unit] = {
    Observable
      .fromTask {
        (Semaphore[Task](pullingBufferSize.value), Semaphore[Task](1)).parTupled
      }
      .flatMap {
        case (txsPullingSemaphore, groupProcessingSemaphore) =>
          Observable
            .repeatEval {
              val txsBatch = utx.selectOrderedTransactionsWithCerts(selectTxPredicate)
              val txsIds   = java.util.Arrays.asList(txsBatch.map(_.tx.id()): _*)
              inProcessTxIds.addAll(txsIds)
              txsBatch
            }
            .doOnNext {
              case txs if txs.isEmpty =>
                Task(log.debug(s"There are no suitable transactions in UTX, retry pulling in $utxCheckDelay")).delayResult(utxCheckDelay)
              case txs =>
                Task(log.trace(s"Processing '${txs.length}' transactions from UTX"))
            }
            .concatMap(txs => Observable.fromIterable(txs)) // Flatten utx batches
            .mapEval { txWithCerts =>
              txsPullingSemaphore.acquire *>
                prepareSetup(txWithCerts)
                  .onErrorRecoverWith {
                    case ex => Task(utx.remove(txWithCerts.tx, Some(ex.toString), mustBeInPool = true)).as(None)
                  }
                  .flatTap {
                    case None =>
                      txsPullingSemaphore.release *>
                        Task(log.debug(s"The environment is not yet ready for transaction '${txWithCerts.tx.id()}' execution"))
                    case _ =>
                      Task(log.trace(s"Tx '${txWithCerts.tx.id()}' has been prepared"))
                  }
            }
            .collect {
              case Some(setup) => setup
            }
            .groupBy(_.groupKey)
            .mergeMap(group => processGroupStream(txsPullingSemaphore, groupProcessingSemaphore, group))
      }
      .doOnSubscriptionCancel { Task(log.debug("Transactions confirmation stream was cancelled")) }
      .completedL
      .executeOn(scheduler)
  }

  private def prepareSetup(txWithCerts: TxWithCerts): Task[Option[TransactionConfirmationSetup]] = {

    prepareCertChain(txWithCerts).flatMap(prepareCrls).flatMap { maybeCertChainWithCrl =>
      // When the contract is ready, we will remove tx from processed for retry.
      def onReady: Coeval[Unit] = Coeval(forgetTxProcessing(txWithCerts.tx.id()))

      (txWithCerts.tx, transactionExecutorOpt) match {
        case (tx: ExecutableTransaction, Some(executor)) =>
          prepareExecutableSetup(maybeCertChainWithCrl, onReady, tx, executor)
        case (tx: ExecutableTransaction, None) =>
          Task.raiseError(DisabledExecutorError(tx.id()))
        case (atomicTx: AtomicTransaction, None) if atomicTx.transactions.exists(_.isInstanceOf[ExecutableTransaction]) =>
          Task.raiseError(DisabledExecutorError(atomicTx.id()))
        case (atomicTx: AtomicTransaction, None) =>
          val innerSetups = atomicTx.transactions.map(SimpleTxSetup(_, maybeCertChainWithCrl))
          Task.pure(Some(AtomicSimpleSetup(atomicTx, innerSetups, maybeCertChainWithCrl)))
        case (atomic: AtomicTransaction, Some(executor)) =>
          OptionT
            .liftF(executor.atomicContractsReady(atomic, onReady))
            .filter(ready => ready)
            .semiflatMap(_ => prepareAtomicComplexSetup(atomic, executor, maybeCertChainWithCrl))
            .value
        case _ =>
          Task.pure(Some(SimpleTxSetup(txWithCerts.tx, maybeCertChainWithCrl)))
      }
    }
  }

  private def prepareExecutableSetup(maybeCertChainWithCrl: Option[(CertChain, CrlCollection)],
                                     onReady: Coeval[Unit],
                                     tx: ExecutableTransaction,
                                     executor: E): Task[Option[TransactionConfirmationSetup]] = {
    def filterReadyContract: OptionT[Task, Boolean] =
      OptionT
        .liftF(executor.contractReady(tx, onReady))
        .filter(ready => ready)

    def defaultCase: Task[Option[DefaultExecutableTxSetup]] =
      filterReadyContract
        .semiflatMap(_ => executor.prepareSetup(tx, maybeCertChainWithCrl))
        .value

    tx match {
      case callTxV6: CallContractTransactionV6 =>
        transactionsAccumulator.contract(ContractId(tx.contractId)).filter(_.isConfidential).map {
          confidentialContractInfo =>
            if (confidentialContractInfo.groupParticipants.contains(ownerKey.toAddress)) {
              filterReadyContract
                .semiflatMap(_ => executor.prepareConfidentialSetup(callTxV6, confidentialContractInfo, maybeCertChainWithCrl))
                .value
            } else {
              Task.pure {
                Some {
                  ConfidentialExecutableUnpermittedSetup(tx, confidentialContractInfo, maybeCertChainWithCrl)
                }
              }
            }
        }.getOrElse(defaultCase)
      case _ =>
        defaultCase
    }
  }

  protected def prepareCrls(maybeCertChain: Option[CertChain]): Task[Option[(CertChain, CrlCollection)]] = Task.pure(None)

  protected def prepareCertChain(txWithCerts: TxWithCerts): Task[Option[CertChain]] = Task.pure(None)

  private def prepareAtomicComplexSetup(atomic: AtomicTransaction,
                                        executor: TransactionsExecutor,
                                        maybeCertChainWithCrl: Option[(CertChain, CrlCollection)]): Task[AtomicComplexSetup] = {
    val innerSetupTasks = atomic.transactions.map {
      case executableTx: ExecutableTransaction => executor.prepareSetup(executableTx, maybeCertChainWithCrl)
      case tx                                  => Task.pure(SimpleTxSetup(tx, maybeCertChainWithCrl))
    }
    Task.pure(AtomicComplexSetup(atomic, innerSetupTasks, maybeCertChainWithCrl))
  }

  protected def processGroupStream(txsPullingSemaphore: Semaphore[Task],
                                   groupExecutionSemaphore: Semaphore[Task],
                                   groupStream: GroupedObservable[GroupKey, TransactionConfirmationSetup]): Observable[Unit] = {
    val parallelism = groupStream.key.groupParallelism
    log.debug(s"Start processing ${groupStream.key.description} with parallelism '$parallelism'")

    val inProgressCounter = AtomicInt(0)

    def acquireGroupProcessing: Task[Unit] = {
      def incrementIfNotZero(old: Int): Int = if (old == 0) 0 else old + 1
      if (inProgressCounter.transformAndGet(incrementIfNotZero) == 0) {
        // Wait until executing group will give access
        groupExecutionSemaphore.acquire.map(_ => inProgressCounter.increment())
      } else {
        // If the group is already executing no need to wait
        Task.unit
      }
    }

    groupStream
      .asyncBoundary(OverflowStrategy.BackPressure(parallelism * 2))
      .doOnNext(_ => acquireGroupProcessing >> txsPullingSemaphore.release)
      .mapParallelUnordered(parallelism) { txSetup =>
        processSetup(txSetup).guarantee {
          Task.defer {
            if (inProgressCounter.decrementAndGet() == 0) {
              groupExecutionSemaphore.release >>
                Task(log.trace(s"Completed ${groupStream.key.description}, switch to next group"))
            } else {
              Task.unit
            }
          }
        }
      }
  }

  protected def processSetup(txSetup: TransactionConfirmationSetup): Task[Unit] = {
    import TransactionsConfirmatory._

    def raiseDisabledExecutorError: Task[Nothing] =
      Task.raiseError(new IllegalStateException("It is impossible to process setup because the executor is disabled"))

    val resultTask = (txSetup, transactionExecutorOpt) match {
      case (SimpleTxSetup(tx, maybeCertChainWithCrl), _)      => processSimpleSetup(tx, maybeCertChainWithCrl)
      case (setup: ConfidentialExecutableUnpermittedSetup, _) => processExecutableSetup(setup)
      case (executableSetup: ExecutableSetup, Some(executor)) => processExecutableSetup(executableSetup, executor)
      case (_: ExecutableSetup, None)                         => raiseDisabledExecutorError
      case (_: AtomicComplexSetup, None)                      => raiseDisabledExecutorError
      case (atomicSetup: AtomicSimpleSetup, _)                => processAtomicSimpleSetup(atomicSetup)
      case (atomicSetup: AtomicComplexSetup, Some(executor))  => processAtomicComplexSetup(atomicSetup, executor)
    }

    resultTask *> Task(processedTxCounter.incrementByType(txSetup.tx))
  }

  def processExecutableSetup(setup: ConfidentialExecutableUnpermittedSetup): Task[Unit]

  @inline
  protected def confirmTx(txWithDiff: TransactionWithDiff): Task[Unit] = {
    confirmedTxsQueue.offer(txWithDiff) *>
      Task(confirmedTxCounter.increment())
  }

  private def processSimpleSetup(tx: Transaction, maybeCertChainWithCrl: Option[(CertChain, CrlCollection)]): Task[Unit] =
    transactionsAccumulator.process(tx, Seq.empty, maybeCertChainWithCrl) match {
      case Right(diff) =>
        confirmTx(TransactionWithDiff(tx, diff))
      case Left(constraintsOverflowError: ConstraintsOverflowError) =>
        Task(log.debug(s"Transaction '${tx.id()}' was discarded because it exceeds the constraints")) *>
          raiseExceptionIfAllConstraintsOverflowError(constraintsOverflowError)
      case Left(MvccConflictError) =>
        Task {
          log.debug(s"Transaction '${tx.id()}' was discarded because it caused MVCC conflict")
          forgetTxProcessing(tx.id())
        }
      case Left(error) =>
        Task(utx.remove(tx, Some(error.toString), mustBeInPool = true))
    }

  private def raiseExceptionIfAllConstraintsOverflowError(constraintsOverflowError: ConstraintsOverflowError): Task[Unit] = {
    constraintsOverflowError match {
      case OneConstraintOverflowError      => Task.unit
      case CriticalConstraintOverflowError => Task.raiseError(new RuntimeException("Critical constraint is overflowed") with NoStackTrace)
    }
  }

  private def processExecutableSetup(setup: ExecutableSetup, executor: TransactionsExecutor): Task[Unit] = {
    executor.processSetup(setup).flatMap { maybeTxWithDiff =>
      maybeTxWithDiff.fold(
        {
          case MvccConflictError => Task(forgetTxProcessing(setup.tx.id()))
          case _                 => Task.unit
        },
        confirmTx
      )
    }
  }

  private def processAtomicSimpleSetup(atomicSetup: AtomicSimpleSetup): Task[Unit] = {
    Task {
      for {
        _ <- transactionsAccumulator.startAtomic()
        _ <-
          atomicSetup.innerSetups.traverse(setup => transactionsAccumulator.processAtomically(setup.tx, Seq.empty, atomicSetup.maybeCertChainWithCrl))
        signedAtomic <- AtomicUtils.addMinerProof(atomicSetup.tx, ownerKey)
        atomicDiff   <- transactionsAccumulator.commitAtomic(signedAtomic, Seq.empty, atomicSetup.maybeCertChainWithCrl)
      } yield TransactionWithDiff(signedAtomic, atomicDiff)
    }.doOnCancel {
      Task {
        log.debug(s"Simple atomic setup processing for tx '${atomicSetup.tx.id()}' was cancelled")
        transactionsAccumulator.cancelAtomic()
      }
    }
      .flatMap {
        case Right(signedAtomicWithDiff) =>
          confirmTx(signedAtomicWithDiff)
        case Left(constraintsOverflowError: ConstraintsOverflowError) =>
          Task {
            log.debug(s"Atomic transaction '${atomicSetup.tx.id()}' was discarded because it exceeds the constraints")
            transactionsAccumulator.rollbackAtomic()
          } *> raiseExceptionIfAllConstraintsOverflowError(constraintsOverflowError)
        case Left(MvccConflictError) =>
          Task {
            log.debug(s"Atomic transaction '${atomicSetup.tx.id()}' was discarded because it caused MVCC conflict")
            transactionsAccumulator.rollbackAtomic()
            forgetTxProcessing(atomicSetup.tx.id())
          }
        case Left(error) =>
          Task {
            transactionsAccumulator.rollbackAtomic()
            utx.remove(atomicSetup.tx, Some(error.toString), mustBeInPool = true)
          }
      }
  }

  private def processAtomicComplexSetup(atomicSetup: AtomicComplexSetup, executor: TransactionsExecutor): Task[Unit] = {
    (for {
      _ <- EitherT.fromEither[Task](transactionsAccumulator.startAtomic())
      innerTxsWithDiff <- atomicSetup.innerSetupTasks
        .traverse { setupTask =>
          EitherT.right[ValidationError](setupTask).flatMap {
            case executableSetup: DefaultExecutableTxSetup =>
              EitherT(executor.processSetup(executableSetup, atomically = true, txContext = TxContext.AtomicInner))
            case SimpleTxSetup(tx, maybeCertChain) =>
              EitherT
                .fromEither[Task](transactionsAccumulator.processAtomically(tx, Seq.empty, maybeCertChain))
                .map { diff =>
                  TransactionWithDiff(tx, diff)
                }
            case _ =>
              EitherT
                .leftT[Task, TransactionWithDiff]
                .apply[ValidationError](
                  GenericError("Wrong internal setup type of atomic")
                )
          }
        }
      executedTxs = innerTxsWithDiff.collect {
        case TransactionWithDiff(executedTx: ExecutedContractTransaction, _) => executedTx
      }
      maybeFailedTx = executedTxs.find {
        case tx: ExecutedContractTransactionV5 if tx.statusCode != 0 => true
        case _                                                       => false
      }
      atomicTx = atomicSetup.tx match {
        case atomic: AtomicTransactionV1 =>
          atomic.copy(
            transactions = maybeFailedTx.fold(atomic.transactions)(exec => List(exec.tx.asInstanceOf[AtomicInnerTransaction]))
          )
        case tx => tx
      }
      atomicWithExecutedTxs = AtomicUtils.addExecutedTxs(atomicTx, maybeFailedTx.fold(executedTxs)(List(_)))
      signedAtomic <- EitherT.fromEither[Task](AtomicUtils.addMinerProof(atomicWithExecutedTxs, ownerKey))
      atomicDiff   <- EitherT.fromEither[Task](transactionsAccumulator.commitAtomic(signedAtomic, Seq.empty, atomicSetup.maybeCertChainWithCrl))
    } yield {
      TransactionWithDiff(signedAtomic, atomicDiff)
    }).value
      .doOnCancel {
        Task {
          log.debug(s"Complex atomic setup processing for tx '${atomicSetup.tx.id()}' was cancelled")
          transactionsAccumulator.cancelAtomic()
        }
      }
      .flatMap {
        case Left(constraintsOverflowError: ConstraintsOverflowError) =>
          Task {
            log.debug(s"Atomic transaction '${atomicSetup.tx.id()}' was discarded because it exceeds the constraints")
            transactionsAccumulator.rollbackAtomic()
          } *> raiseExceptionIfAllConstraintsOverflowError(constraintsOverflowError)
        case Left(error: InvalidValidationProofs) =>
          Task {
            log.debug(s"Atomic transaction '${atomicSetup.tx.id()}' was discarded, cause: $error")
            transactionsAccumulator.rollbackAtomic()
          }
        case Left(MvccConflictError) =>
          Task {
            log.debug(s"Atomic transaction '${atomicSetup.tx.id()}' was discarded because it caused MVCC conflict")
            transactionsAccumulator.rollbackAtomic()
            forgetTxProcessing(atomicSetup.tx.id())
          }
        case Left(error) =>
          Task {
            transactionsAccumulator.rollbackAtomic()
            utx.remove(atomicSetup.tx, Some(error.toString), mustBeInPool = true)
          }
        case Right(signedAtomicWithDiff) =>
          confirmTx(signedAtomicWithDiff)
      }
  }

  @inline
  private def forgetTxProcessing(txId: ByteStr): Unit = inProcessTxIds.remove(txId)
}

object TransactionsConfirmatory {

  private implicit class TxCounterMetric(private val counter: CounterMetric) extends AnyVal {
    def incrementByType(tx: Transaction): Unit = {
      counter.refine("transaction-type", tx.builder.typeId.toString).increment()
    }
  }

  sealed abstract class Error(message: String) extends RuntimeException(message)
  object Error {
    case class DisabledExecutorError(txId: ByteStr)                          extends Error(s"Impossible to process tx '$txId' setup because the executor is disabled")
    case class TxOwnerExtractionError(txId: ByteStr, cause: ValidationError) extends Error(s"Failed to extract transaction '$txId' owner: $cause")
    case class TxOwnerCertNotFoundError(txId: ByteStr)                       extends Error(s"Transaction '$txId' owner certificate not found")
    case class EmptyTxCertChainError(txId: ByteStr)                          extends Error(s"Empty cert chain for transaction '$txId'")
    case class IssuerCertNotFoundError(issuerDn: String)                     extends Error(s"Issuer certificate for DN '$issuerDn' not found")
    case class CrlDownloadError(reason: String)                              extends Error(s"Failed to download crls: $reason")
  }
}

case class TransactionWithDiff(tx: Transaction, diff: Diff)

class MinerTransactionsConfirmatory(override val transactionsAccumulator: TransactionsAccumulator,
                                    val transactionExecutorOpt: Option[MinerTransactionsExecutor],
                                    val utx: UtxPool,
                                    val pullingBufferSize: PositiveInt,
                                    val utxCheckDelay: FiniteDuration,
                                    val ownerKey: PrivateKeyAccount,
                                    val time: Time,
                                    val blockchain: Blockchain,
                                    val contractValidatorResultsStoreOpt: Option[ContractValidatorResultsStore],
                                    val keyBlockId: ByteStr,
                                    override val confidentialRocksDBStorage: ConfidentialRocksDBStorage,
                                    override val confidentialStateUpdater: ConfidentialStateUpdater)(implicit val scheduler: Scheduler)
    extends TransactionsConfirmatory[MinerTransactionsExecutor] with ConfidentialTxPredicateOps[MinerTransactionsExecutor]
    with ConfidentialUnpermittedSetupProcess {

  override protected def selectTxPredicate(transaction: Transaction): Boolean = super.selectTxPredicate(transaction) && {
    contractInfoIfNeedProcessConfidentially(transaction) match {
      case Some((contractInfo: ContractInfo, exTx: CallContractTransactionV6)) =>
        if (contractInfo.groupParticipants.contains(nodeOwnerAddress)) {
          callTxHasRequiredInputCommitment(exTx) && confidentialStateUpdater.isSynchronized
        } else {
          selectPredicateDefault(transaction, contractInfo.groupParticipants)
        }
      case _ =>
        selectPredicateDefault(transaction) // non-confidential case
    }
  }

  private def selectPredicateDefault(transaction: Transaction, confidentialGroupParticipants: Set[Address] = Set.empty): Boolean = {
    (transaction, transactionExecutorOpt) match {
      case (tx: ExecutableTransaction, Some(executor)) =>
        executor.selectExecutableTxPredicate(tx, confidentialGroupParticipants = confidentialGroupParticipants)
      case (_: ExecutableTransaction, None) =>
        false
      case (atomicTx: AtomicTransaction, None) if atomicTx.transactions.exists(_.isInstanceOf[ExecutableTransaction]) =>
        false
      case (atomicTx: AtomicTransaction, Some(executor)) =>
        // Accumulates validation policies for the case when the transaction depends on the validation policy specified
        // in the previous transaction.
        val (result, _) = atomicTx.transactions.foldLeft(true -> Map.empty[ByteStr, ValidationPolicy]) {
          case ((result, policiesAcc), createTxWithValidationPolicy: CreateContractTransaction with ValidationPolicySupport) =>
            val nextPoliciesAcc = policiesAcc + (createTxWithValidationPolicy.id() -> createTxWithValidationPolicy.validationPolicy)
            (result && executor.selectExecutableTxPredicate(createTxWithValidationPolicy,
                                                            policiesAcc,
                                                            confidentialGroupParticipants = confidentialGroupParticipants)) -> nextPoliciesAcc
          case ((result, policiesAcc), createTx: CreateContractTransaction with AtomicInnerTransaction) =>
            val nextPoliciesAcc = policiesAcc + (createTx.id() -> ValidationPolicy.Default)
            (result && executor.selectExecutableTxPredicate(createTx,
                                                            policiesAcc,
                                                            confidentialGroupParticipants = confidentialGroupParticipants)) -> nextPoliciesAcc
          case ((result, policiesAcc), executableTx: ExecutableTransaction) =>
            (result && executor.selectExecutableTxPredicate(executableTx,
                                                            policiesAcc,
                                                            confidentialGroupParticipants = confidentialGroupParticipants)) -> policiesAcc
          case ((result, policiesAcc), _) =>
            result -> policiesAcc
        }
        result
      case _ => true
    }
  }

  override def processExecutableSetup(setup: ConfidentialExecutableUnpermittedSetup): Task[Unit] = {
    // TODO npoperechnyi: wtf is this
    processConfidentialExecutableUnpermittedSetup(setup).fold(
      error => Task(log.debug(s"Error during process ConfidentialExecutableUnpermittedSetup: '$error'")),
      confirmTx
    )
  }

}

class ValidatorTransactionsConfirmatory(val transactionsAccumulator: TransactionsAccumulator,
                                        val transactionExecutorOpt: Option[ValidatorTransactionsExecutor],
                                        val utx: UtxPool,
                                        val pullingBufferSize: PositiveInt,
                                        val utxCheckDelay: FiniteDuration,
                                        val ownerKey: PrivateKeyAccount,
                                        override val confidentialRocksDBStorage: ConfidentialRocksDBStorage,
                                        override val confidentialStateUpdater: ConfidentialStateUpdater)(implicit val scheduler: Scheduler)
    extends TransactionsConfirmatory[ValidatorTransactionsExecutor] with ConfidentialTxPredicateOps[ValidatorTransactionsExecutor] {

  override def confirmTx(txWithDiff: TransactionWithDiff): Task[Unit] = {
    confirmedTxCounter.increment()
    Task.unit
  }

  override def selectTxPredicate(transaction: Transaction): Boolean = super.selectTxPredicate(transaction) && {
    contractInfoIfNeedProcessConfidentially(transaction) match {
      case Some((contractInfo: ContractInfo, exTx: ExecutableTransaction)) =>
        contractInfo.groupParticipants.contains(nodeOwnerAddress) && callTxHasRequiredInputCommitment(exTx) && confidentialStateUpdater.isSynchronized
      case _ => true // if tx is not confidential and is not already being processed ( super.selectTxPredicate ) then select
    }
  }

  override def processExecutableSetup(setup: ConfidentialExecutableUnpermittedSetup): Task[Unit] =
    Task.raiseError(new IllegalStateException("Validator does not process ConfidentialExecutableUnpermittedSetup"))

}

trait ConfidentialTxPredicateOps[E <: TransactionsExecutor] { _: TransactionsConfirmatory[E] =>
  protected def confidentialRocksDBStorage: ConfidentialRocksDBStorage
  protected def confidentialStateUpdater: ConfidentialStateUpdater

  protected val nodeOwnerAddress: Address = Address.fromPublicKey(ownerKey.publicKey)

  protected def callTxHasRequiredInputCommitment(exTx: ExecutableTransaction): Boolean =
    exTx match {
      case tx: ConfidentialDataInCallContractSupported => tx.inputCommitment.fold(false)(confidentialRocksDBStorage.inputExists)
      case _                                           => false
    }

  protected def contractInfoIfNeedProcessConfidentially(transaction: Transaction): Option[(ContractInfo, ExecutableTransaction)] =
    (transaction, transactionExecutorOpt) match {
      case (exTx: ExecutableTransaction, Some(transactionExecutor)) =>
        transactionExecutor.contractInfo(exTx).toOption.filter(_.isConfidential).map(_ -> exTx)
      case _ => None
    }
}
