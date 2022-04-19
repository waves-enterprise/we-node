package com.wavesenterprise.mining

import cats.data.{EitherT, OptionT}
import cats.implicits._
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.docker.{MinerTransactionsExecutor, TransactionsExecutor, ValidatorTransactionsExecutor}
import com.wavesenterprise.settings.PositiveInt
import com.wavesenterprise.state.{ByteStr, Diff}
import com.wavesenterprise.transaction.ValidationError.{ConstraintsOverflowError, GenericError, InvalidValidationProofs, MvccConflictError}
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.docker.{
  CreateContractTransaction,
  CreateContractTransactionV4,
  ExecutableTransaction,
  ExecutedContractTransaction
}
import com.wavesenterprise.utils.ScorexLogging
import com.wavesenterprise.utx.UtxPool
import monix.catnap.{ConcurrentQueue, Semaphore}
import monix.eval.Task
import monix.execution.atomic.AtomicInt
import monix.execution.{BufferCapacity, ChannelType, Scheduler}
import monix.reactive.observables.GroupedObservable
import monix.reactive.{Observable, OverflowStrategy}

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration._
import scala.util.{Left, Right}

/**
  * Processes transactions from UTX pool and provides the stream of confirmed transactions.
  */
trait TransactionsConfirmatory[E <: TransactionsExecutor] extends ScorexLogging {

  def transactionExecutorOpt: Option[E]

  protected def transactionsAccumulator: TransactionsAccumulator
  protected def utx: UtxPool
  protected def pullingBufferSize: PositiveInt
  protected def utxCheckDelay: FiniteDuration
  protected def ownerKey: PrivateKeyAccount

  implicit protected def scheduler: Scheduler

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
              val txsBatch = utx.selectOrderedTransactions(selectTxPredicate)
              val txsIds   = java.util.Arrays.asList(txsBatch.map(_.id()): _*)
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
            .mapEval(tx => txsPullingSemaphore.acquire *> prepareSetup(tx))
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

  protected def prepareSetup(transaction: Transaction): Task[Option[TransactionConfirmationSetup]] = {

    def raiseDisabledExecutorError =
      Task.raiseError(new IllegalStateException("It is impossible to prepare confirmation setup because the executor is disabled"))

    (transaction, transactionExecutorOpt) match {
      case (tx: ExecutableTransaction, Some(executor)) =>
        OptionT
          .liftF(executor.contractReady(tx))
          .filter(ready => ready)
          .semiflatMap(_ => executor.prepareSetup(tx))
          .value
      case (_: ExecutableTransaction, None) =>
        raiseDisabledExecutorError
      case (atomicTx: AtomicTransaction, None) if atomicTx.transactions.exists(_.isInstanceOf[ExecutableTransaction]) =>
        raiseDisabledExecutorError
      case (atomicTx: AtomicTransaction, None) =>
        val innerSetups = atomicTx.transactions.map(SimpleTxSetup(_))
        Task.pure(Some(AtomicSimpleSetup(atomicTx, innerSetups)))
      case (atomic: AtomicTransaction, Some(executor)) =>
        OptionT
          .liftF(executor.atomicContractsReady(atomic))
          .filter(ready => ready)
          .semiflatMap(_ => prepareAtomicComplexSetup(atomic, executor))
          .value
      case _ =>
        Task.pure(Some(SimpleTxSetup(transaction)))
    }
  }

  private def prepareAtomicComplexSetup(atomic: AtomicTransaction, executor: TransactionsExecutor): Task[AtomicComplexSetup] = {
    val innerSetupTasks = atomic.transactions.map {
      case executableTx: ExecutableTransaction => executor.prepareSetup(executableTx)
      case tx                                  => Task.pure(SimpleTxSetup(tx))
    }
    Task.pure(AtomicComplexSetup(atomic, innerSetupTasks))
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
    def raiseDisabledExecutorError =
      Task.raiseError(new IllegalStateException("It is impossible to process setup because the executor is disabled"))

    (txSetup, transactionExecutorOpt) match {
      case (SimpleTxSetup(tx), _)                               => processSimpleSetup(tx)
      case (executableSetup: ExecutableTxSetup, Some(executor)) => processExecutableSetup(executableSetup, executor)
      case (_: ExecutableTxSetup, None)                         => raiseDisabledExecutorError
      case (_: AtomicComplexSetup, None)                        => raiseDisabledExecutorError
      case (atomicSetup: AtomicSimpleSetup, _)                  => processAtomicSimpleSetup(atomicSetup)
      case (atomicSetup: AtomicComplexSetup, Some(executor))    => processAtomicComplexSetup(atomicSetup, executor)
    }
  }

  @inline
  protected def confirmTx(txWithDiff: TransactionWithDiff): Task[Unit] = confirmedTxsQueue.offer(txWithDiff)

  private def processSimpleSetup(tx: Transaction): Task[Unit] =
    transactionsAccumulator.process(tx) match {
      case Right(diff) =>
        confirmTx(TransactionWithDiff(tx, diff))
      case Left(ConstraintsOverflowError) =>
        Task(log.debug(s"Transaction '${tx.id()}' was discarded because it exceeds the constraints"))
      case Left(MvccConflictError) =>
        Task {
          log.debug(s"Transaction '${tx.id()}' was discarded because it caused MVCC conflict")
          forgetTxProcessing(tx.id())
        }
      case Left(error) =>
        Task(utx.remove(tx, Some(error.toString), mustBeInPool = true))
    }

  private def processExecutableSetup(setup: ExecutableTxSetup, executor: TransactionsExecutor): Task[Unit] = {
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
        _            <- transactionsAccumulator.startAtomic()
        _            <- atomicSetup.innerSetups.traverse(setup => transactionsAccumulator.processAtomically(setup.tx))
        signedAtomic <- AtomicUtils.addMinerProof(atomicSetup.tx, ownerKey)
        atomicDiff   <- transactionsAccumulator.commitAtomic(signedAtomic)
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
        case Left(ConstraintsOverflowError) =>
          Task {
            log.debug(s"Atomic transaction '${atomicSetup.tx.id()}' was discarded because it exceeds the constraints")
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
      }
  }

  private def processAtomicComplexSetup(atomicSetup: AtomicComplexSetup, executor: TransactionsExecutor): Task[Unit] = {
    (for {
      _ <- EitherT.fromEither[Task](transactionsAccumulator.startAtomic())
      innerTxsWithDiff <- atomicSetup.innerSetupTasks
        .traverse { setupTask =>
          EitherT.right[ValidationError](setupTask).flatMap {
            case executableSetup: ExecutableTxSetup =>
              EitherT(executor.processSetup(executableSetup, atomically = true))
            case SimpleTxSetup(tx) =>
              EitherT
                .fromEither[Task](transactionsAccumulator.processAtomically(tx))
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
      atomicWithExecutedTxs = AtomicUtils.addExecutedTxs(atomicSetup.tx, executedTxs)
      signedAtomic <- EitherT.fromEither[Task](AtomicUtils.addMinerProof(atomicWithExecutedTxs, ownerKey))
      atomicDiff   <- EitherT.fromEither[Task](transactionsAccumulator.commitAtomic(signedAtomic))
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
        case Left(ConstraintsOverflowError) =>
          Task {
            log.debug(s"Atomic transaction '${atomicSetup.tx.id()}' was discarded because it exceeds the constraints")
            transactionsAccumulator.rollbackAtomic()
          }
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

case class TransactionWithDiff(tx: Transaction, diff: Diff)

class MinerTransactionsConfirmatory(val transactionsAccumulator: TransactionsAccumulator,
                                    val transactionExecutorOpt: Option[MinerTransactionsExecutor],
                                    val utx: UtxPool,
                                    val pullingBufferSize: PositiveInt,
                                    val utxCheckDelay: FiniteDuration,
                                    val ownerKey: PrivateKeyAccount)(implicit val scheduler: Scheduler)
    extends TransactionsConfirmatory[MinerTransactionsExecutor] {

  override protected def selectTxPredicate(transaction: Transaction): Boolean = {
    super.selectTxPredicate(transaction) &&
    ((transaction, transactionExecutorOpt) match {
      case (tx: ExecutableTransaction, Some(executor)) =>
        executor.selectExecutableTxPredicate(tx)
      case (_: ExecutableTransaction, None) =>
        false
      case (atomicTx: AtomicTransaction, None) if atomicTx.transactions.exists(_.isInstanceOf[ExecutableTransaction]) =>
        false
      case (atomicTx: AtomicTransaction, Some(executor)) =>
        // Accumulates validation policies for the case when the transaction depends on the validation policy specified
        // in the previous transaction.
        val (result, _) = atomicTx.transactions.foldLeft(true -> Map.empty[ByteStr, ValidationPolicy]) {
          case ((result, policiesAcc), createTxV4: CreateContractTransactionV4) =>
            val nextPoliciesAcc = policiesAcc + (createTxV4.id() -> createTxV4.validationPolicy)
            (result && executor.selectExecutableTxPredicate(createTxV4, policiesAcc)) -> nextPoliciesAcc
          case ((result, policiesAcc), createTx: CreateContractTransaction with AtomicInnerTransaction) =>
            val nextPoliciesAcc = policiesAcc + (createTx.id() -> ValidationPolicy.Default)
            (result && executor.selectExecutableTxPredicate(createTx, policiesAcc)) -> nextPoliciesAcc
          case ((result, policiesAcc), executableTx: ExecutableTransaction) =>
            (result && executor.selectExecutableTxPredicate(executableTx, policiesAcc)) -> policiesAcc
          case ((result, policiesAcc), _) =>
            result -> policiesAcc
        }
        result
      case _ => true
    })
  }
}

class ValidatorTransactionsConfirmatory(val transactionsAccumulator: TransactionsAccumulator,
                                        val transactionExecutorOpt: Option[ValidatorTransactionsExecutor],
                                        val utx: UtxPool,
                                        val pullingBufferSize: PositiveInt,
                                        val utxCheckDelay: FiniteDuration,
                                        val ownerKey: PrivateKeyAccount)(implicit val scheduler: Scheduler)
    extends TransactionsConfirmatory[ValidatorTransactionsExecutor] {
  override def confirmTx(txWithDiff: TransactionWithDiff): Task[Unit] = Task.unit
}
