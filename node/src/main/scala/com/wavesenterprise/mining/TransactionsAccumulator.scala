package com.wavesenterprise.mining

import cats.kernel.Monoid
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.acl.PermissionValidator
import com.wavesenterprise.database.docker.KeysRequest
import com.wavesenterprise.mining.TransactionsAccumulator.KeysReadingInfo._
import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.settings.{BlockchainSettings, WESettings}
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.state.diffs.TransactionDiffer
import com.wavesenterprise.state.reader.{CompositeBlockchainWithNG, ReadWriteLockingBlockchain}
import com.wavesenterprise.state.{Blockchain, ByteStr, DataEntry, Diff, MiningConstraintsHolder, NG}
import com.wavesenterprise.transaction.ValidationError.{ConstraintsOverflowError, GenericError, MvccConflictError}
import com.wavesenterprise.transaction.docker.{ExecutedContractData, ExecutedContractTransaction}
import com.wavesenterprise.transaction.{AtomicTransaction, Transaction, ValidationError}
import com.wavesenterprise.utils.Time
import monix.execution.atomic.AtomicInt
import scorex.util.ScorexLogging

import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable.SortedMap
import scala.concurrent.duration.FiniteDuration

/**
  * Processes transactions and collects their [[Diff]]s into one.
  */
class TransactionsAccumulator(ng: NG,
                              blockchain: Blockchain,
                              blockchainSettings: BlockchainSettings,
                              permissionValidator: PermissionValidator,
                              time: Time,
                              miner: PublicKeyAccount,
                              txExpireTimeout: FiniteDuration,
                              miningConstraints: MiningConstraints)
    extends ReadWriteLockingBlockchain
    with ScorexLogging {

  import TransactionsAccumulator._

  private[this] val snapshotIdCounter = AtomicInt(0)

  @inline
  def currentSnapshotId: SnapshotId = snapshotIdCounter.get()

  @inline
  private def nextSnapshotId(): SnapshotId = snapshotIdCounter.incrementAndGet()

  protected[this] var state: Blockchain = blockchain
  private[this] var diff                = Diff.empty
  private[this] var constraints         = miningConstraints.total

  private[this] var snapshots = SortedMap(currentSnapshotId -> Snapshot(diff, state))

  private[this] var diffCheckpoint        = diff
  private[this] var constraintsCheckpoint = constraints
  private[this] var snapshotsCheckpoint   = snapshots

  private[this] val txToReadingDescriptor = new ConcurrentHashMap[TxId, ReadingDescriptor]

  private case class DiffWithConstraints(diff: Diff, constraints: MiningConstraint)

  protected val txDiffer: TransactionDiffer = TransactionDiffer(
    settings = blockchainSettings,
    permissionValidator = permissionValidator,
    prevBlockTimestamp = ng.lastPersistenceBlock.map(_.timestamp),
    currentBlockTimestamp = ng.currentBaseBlock.map(_.timestamp).getOrElse(time.correctedTime()),
    currentBlockHeight = ng.height,
    txExpireTimeout = txExpireTimeout,
    minerOpt = Some(miner)
  )

  private[this] var processingAtomic: Boolean = false

  def process(tx: Transaction, maybeCertChain: Option[CertChain]): Either[ValidationError, Diff] = writeLock {
    if (processingAtomic) {
      Left(GenericError("Can't process transaction during atomic transaction mining"))
    } else {
      processTransaction(tx, maybeCertChain)
    }
  }

  private def processTransaction(tx: Transaction, maybeCertChain: Option[CertChain], atomically: Boolean = false): Either[ValidationError, Diff] = {
    val conflictFound           = findConflicts(tx)
    lazy val updatedConstraints = constraints.put(state, tx)

    if (conflictFound) {
      Left(MvccConflictError)
    } else if (updatedConstraints.isOverfilled) {
      Left(ConstraintsOverflowError)
    } else {
      txDiffer(state, tx, maybeCertChain, atomically).map { txDiff =>
        constraints = updatedConstraints
        diff = Monoid.combine(diff, txDiff)
        state = CompositeBlockchainWithNG(ng, blockchain, diff)
        snapshots = snapshots.updated(nextSnapshotId(), Snapshot(diff, state))
        txDiff
      }
    }
  }

  private def findConflicts(tx: Transaction): Boolean = {
    tx match {
      case executedTx: ExecutedContractTransaction =>
        val executableTx   = executedTx.tx
        val executableTxId = executableTx.id()

        val result = Option(txToReadingDescriptor.get(executableTxId))
          .fold {
            log.trace(s"Reading descriptor for tx '$executableTxId' not found")
            false
          } { readingDescriptor =>
            val contractIdToKeysReadingInfo = readingDescriptor.optimizedKeysReadingInfoByContract
            val readContractIds             = contractIdToKeysReadingInfo.keySet

            log.trace(
              s"Reading descriptor for tx '$executableTxId': '$readingDescriptor', after optimization:'$contractIdToKeysReadingInfo'" +
                s", actual snapshot '$currentSnapshotId'"
            )

            snapshots.from(readingDescriptor.snapshotId + 1).values.map(_.diff).exists { snapshotDiff =>
              snapshotDiff.contracts.keySet.exists(readContractIds.contains) ||
              readContractIds.exists { readContractId =>
                snapshotDiff.contractsData.get(readContractId).fold(false) { executionResult =>
                  contractIdToKeysReadingInfo(readContractId).exists {
                    case AllPossible(_)            => true
                    case SpecificSet(_, keySet)    => executionResult.data.keySet.exists(keySet.contains)
                    case ByPredicate(_, predicate) => executionResult.data.keys.exists(predicate)
                  }
                }
              }
            }
          }

        txToReadingDescriptor.remove(executableTxId)
        result
      case _ => false
    }
  }

  def startAtomic(): Either[ValidationError, Unit] = writeLock {
    if (processingAtomic) {
      Left(GenericError("Atomic transaction mining has been already started"))
    } else {
      diffCheckpoint = diff
      constraintsCheckpoint = constraints
      snapshotsCheckpoint = snapshots
      processingAtomic = true
      Right(())
    }
  }

  def processAtomically(tx: Transaction, maybeCertChain: Option[CertChain]): Either[ValidationError, Diff] = writeLock {
    if (processingAtomic) {
      processTransaction(tx, maybeCertChain, atomically = true)
    } else {
      Left(GenericError("Atomic transaction mining is not started"))
    }
  }

  def commitAtomic(tx: AtomicTransaction, maybeCertChain: Option[CertChain]): Either[ValidationError, Diff] = writeLock {
    for {
      _ <- Either.cond(processingAtomic, (), GenericError("Atomic transaction mining is not started"))
      _ = {
        diff = diffCheckpoint
        constraints = constraintsCheckpoint
        snapshots = snapshotsCheckpoint
        state = CompositeBlockchainWithNG(ng, blockchain, diff)
      }
      atomicTxDiff <- processTransaction(tx, maybeCertChain)
    } yield {
      processingAtomic = false
      atomicTxDiff
    }
  }

  def rollbackAtomic(): Unit = writeLock {
    if (processingAtomic) {
      diff = diffCheckpoint
      constraints = constraintsCheckpoint
      state = CompositeBlockchainWithNG(ng, blockchain, diff)
      snapshots = snapshotsCheckpoint
      processingAtomic = false
    } else {
      Left(GenericError("Atomic transaction mining is not started"))
    }
  }

  def cancelAtomic(): Unit = writeLock {
    if (processingAtomic) rollbackAtomic()
  }

  override def contractKeys(request: KeysRequest, readingContext: ContractReadingContext): Vector[String] = {
    readLock {
      val newKeysReadingInfo = request.keysFilter.fold[KeysReadingInfo](AllPossible(request.contractId)) {
        ByPredicate(request.contractId, _)
      }

      readingContext match {
        case ContractReadingContext.Default =>
          state.contractKeys(request, readingContext)
        case ContractReadingContext.TransactionExecution(txId) =>
          contractReadingWithSnapshot(txId, newKeysReadingInfo) { blockchainSnapshot =>
            blockchainSnapshot.contractKeys(request, readingContext)
          }
      }

    }
  }

  override def contractData(contractId: ContractId, readingContext: ContractReadingContext): ExecutedContractData =
    readLock {
      val newKeysReadingInfo = AllPossible(contractId)

      readingContext match {
        case ContractReadingContext.Default =>
          state.contractData(contractId, readingContext)
        case ContractReadingContext.TransactionExecution(txId) =>
          contractReadingWithSnapshot(txId, newKeysReadingInfo) { blockchainSnapshot =>
            blockchainSnapshot.contractData(contractId, readingContext)
          }
      }

    }

  override def contractData(contractId: ContractId, key: String, readingContext: ContractReadingContext): Option[DataEntry[_]] =
    readLock {
      val newKeysReadingInfo = SpecificSet(contractId, Set(key))

      readingContext match {
        case ContractReadingContext.Default =>
          state.contractData(contractId, key, readingContext)
        case ContractReadingContext.TransactionExecution(txId) =>
          contractReadingWithSnapshot(txId, newKeysReadingInfo) { blockchainSnapshot =>
            blockchainSnapshot.contractData(contractId, key, readingContext)
          }
      }

    }

  override def contractData(contractId: ContractId, keys: Iterable[String], readingContext: ContractReadingContext): ExecutedContractData =
    readLock {
      val newKeysReadingInfo = SpecificSet(contractId, keys.toSet)

      readingContext match {
        case ContractReadingContext.Default =>
          state.contractData(contractId, keys, readingContext)
        case ContractReadingContext.TransactionExecution(txId) =>
          contractReadingWithSnapshot(txId, newKeysReadingInfo) { blockchainSnapshot =>
            blockchainSnapshot.contractData(contractId, keys, readingContext)
          }
      }
    }

  private def contractReadingWithSnapshot[T](executableTxId: TxId, newKeysReadingInfo: KeysReadingInfo)(readingBlock: Blockchain => T): T =
    readLock {
      val ReadingDescriptor(snapshotId, _) = txToReadingDescriptor.compute(
        executableTxId,
        (_, maybeExistingDescriptor) =>
          Option(maybeExistingDescriptor).fold(ReadingDescriptor(currentSnapshotId, List(newKeysReadingInfo))) { oldValue =>
            oldValue.copy(keysReadingInfo = newKeysReadingInfo :: oldValue.keysReadingInfo)
        }
      )

      snapshots
        .get(snapshotId)
        .fold(throw new IllegalStateException(s"Snapshot '$snapshotId' for executable transaction '$executableTxId' not found")) { snapshot =>
          readingBlock(snapshot.blockchain)
        }
    }

}

object TransactionsAccumulator {
  type SnapshotId = Int
  type TxId       = ByteStr
  type ContractId = ByteStr

  case class Snapshot(diff: Diff, blockchain: Blockchain)

  sealed trait KeysReadingInfo {
    def contractId: ContractId
  }

  object KeysReadingInfo {
    case class AllPossible(contractId: ContractId)                           extends KeysReadingInfo
    case class SpecificSet(contractId: ContractId, values: Set[String])      extends KeysReadingInfo
    case class ByPredicate(contractId: ContractId, value: String => Boolean) extends KeysReadingInfo
  }

  case class ReadingDescriptor(snapshotId: SnapshotId, keysReadingInfo: List[KeysReadingInfo]) {

    def optimizedKeysReadingInfoByContract: Map[ContractId, List[KeysReadingInfo]] =
      keysReadingInfo.groupBy(_.contractId).map((optimizeContractGroup _).tupled)

    private def optimizeContractGroup(contractId: ContractId, keysReadingGroup: List[KeysReadingInfo]): (ContractId, List[KeysReadingInfo]) = {
      val (allPossibleKeys, unionKeySet, maybePredicate) =
        keysReadingGroup
          .foldLeft((false, Set.empty[String], Option.empty[String => Boolean])) {
            case ((true, _, _), _) =>
              (true, Set.empty, None)
            case ((_, _, _), AllPossible(_)) =>
              (true, Set.empty, None)
            case ((allPossibleKeysAcc, keySetAcc, predicateAcc), SpecificSet(_, newSet)) =>
              (allPossibleKeysAcc, keySetAcc ++ newSet, predicateAcc)
            case ((allPossibleKeysAcc, keySetAcc, predicateAcc), ByPredicate(_, newPredicate)) =>
              val updatedPredicateAcc = Some {
                predicateAcc.fold(newPredicate) { existingPredicate =>
                  { input =>
                    existingPredicate(input) || newPredicate(input)
                  }
                }
              }

              (allPossibleKeysAcc, keySetAcc, updatedPredicateAcc)
          }

      val optimizedKeysReadingInfo = List.concat(
        if (allPossibleKeys) List(AllPossible(contractId)) else Nil,
        List(SpecificSet(contractId, unionKeySet)).filter(_.values.nonEmpty),
        maybePredicate.fold(List.empty[KeysReadingInfo])(predicate => List(ByPredicate(contractId, predicate)))
      )

      contractId -> optimizedKeysReadingInfo
    }
  }
}

class TransactionsAccumulatorProvider(ng: NG,
                                      persistentBlockchain: Blockchain,
                                      settings: WESettings,
                                      permissionValidator: PermissionValidator,
                                      time: Time,
                                      miner: PublicKeyAccount) {

  def build(maybeUpdatedBlockchain: Option[Blockchain with MiningConstraintsHolder] = None): TransactionsAccumulator = {
    val initialConstraints = MiningConstraints(ng, ng.height, settings.miner.maxBlockSizeInBytes, Some(settings.miner))

    val resultMiningConstraints = maybeUpdatedBlockchain match {
      case Some(constraintsHolder) => initialConstraints.copy(total = constraintsHolder.restTotalConstraint)
      case None                    => initialConstraints
    }

    val resultBlockchain = maybeUpdatedBlockchain.getOrElse(persistentBlockchain)

    buildAccumulator(resultMiningConstraints, resultBlockchain)
  }

  protected def buildAccumulator(resultMiningConstraints: MiningConstraints, resultBlockchain: Blockchain): TransactionsAccumulator =
    new TransactionsAccumulator(
      ng = ng,
      blockchain = resultBlockchain,
      blockchainSettings = settings.blockchain,
      permissionValidator = permissionValidator,
      time = time,
      miner = miner,
      txExpireTimeout = settings.utx.txExpireTimeout,
      miningConstraints = resultMiningConstraints
    )
}
