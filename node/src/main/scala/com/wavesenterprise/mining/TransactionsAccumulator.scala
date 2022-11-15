package com.wavesenterprise.mining

import cats.kernel.Monoid
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.acl.PermissionValidator
import com.wavesenterprise.database.docker.KeysRequest
import com.wavesenterprise.mining.TransactionsAccumulator.KeysReadingInfo._
import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.mining.TransactionsAccumulator.Currency.{CustomAsset, West}
import com.wavesenterprise.settings.{BlockchainSettings, WESettings}
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.state.diffs.TransactionDiffer
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state.diffs.docker.ExecutedContractTransactionDiff.{ContractTxExecutorType, MiningExecutor}
import com.wavesenterprise.state.reader.{CompositeBlockchainWithNG, ReadWriteLockingBlockchain}
import com.wavesenterprise.state.{Blockchain, ByteStr, ContractId => StateContractId, DataEntry, Diff, MiningConstraintsHolder, NG}
import com.wavesenterprise.transaction.ValidationError.{ConstraintsOverflowError, GenericError, MvccConflictError}
import com.wavesenterprise.transaction.docker.{ExecutedContractData, ExecutedContractTransaction}
import com.wavesenterprise.transaction.{AssetId, AtomicTransaction, Transaction, ValidationError}
import com.wavesenterprise.utils.Time
import com.wavesenterprise.utils.pki.CrlCollection
import monix.execution.atomic.AtomicInt
import scorex.util.ScorexLogging

import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable.{Set, SortedMap}
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
                              miningConstraints: MiningConstraints,
                              contractTxExecutor: ContractTxExecutorType = MiningExecutor)
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
  private[this] var snapshotIdCheckpoint  = currentSnapshotId

  private[this] val txToReadingDescriptor = new ConcurrentHashMap[TxId, ReadingDescriptor]

  private case class DiffWithConstraints(diff: Diff, constraints: MiningConstraint)

  protected val txDiffer: TransactionDiffer = TransactionDiffer(
    settings = blockchainSettings,
    permissionValidator = permissionValidator,
    prevBlockTimestamp = ng.lastPersistenceBlock.map(_.timestamp),
    currentBlockTimestamp = ng.currentBaseBlock.map(_.timestamp).getOrElse(time.correctedTime()),
    currentBlockHeight = ng.height,
    txExpireTimeout = txExpireTimeout,
    minerOpt = Some(miner),
    contractTxExecutor = contractTxExecutor
  )

  private[this] var processingAtomic: Boolean = false

  def process(tx: Transaction, maybeCertChainWithCrl: Option[(CertChain, CrlCollection)]): Either[ValidationError, Diff] = writeLock {
    if (processingAtomic) {
      Left(GenericError("Can't process transaction during atomic transaction mining"))
    } else {
      processTransaction(tx, maybeCertChainWithCrl)
    }
  }

  private def processTransaction(tx: Transaction,
                                 maybeCertChainWithCrl: Option[(CertChain, CrlCollection)],
                                 atomically: Boolean = false): Either[ValidationError, Diff] = {
    val conflictFound           = findConflicts(tx)
    lazy val updatedConstraints = constraints.put(state, tx)

    if (conflictFound) {
      Left(MvccConflictError)
    } else if (updatedConstraints.isOverfilled) {
      Left(ConstraintsOverflowError)
    } else {
      txDiffer(state, tx, maybeCertChainWithCrl, atomically).map { txDiff =>
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
              snapshotDiff.contracts.keySet.exists(contractId => readContractIds.contains(contractId.byteStr)) ||
              readContractIds.exists { readContractId =>
                val checkDataEntryReadingConflicts = snapshotDiff.contractsData.get(readContractId).fold(false) { executionResult =>
                  contractIdToKeysReadingInfo(readContractId).dataKeysReadingInfo.exists {
                    case AllPossibleDataEntries(_)            => true
                    case SpecificDataEntriesSet(_, keySet)    => executionResult.data.keySet.exists(keySet.contains)
                    case DataEntriesByPredicate(_, predicate) => executionResult.data.keys.exists(predicate)
                  }
                }

                // 'def' is just an optimisation to compute this only when 'checkDataEntryReadingConflicts == false'
                def checkContractBalanceReadingConflicts =
                  snapshotDiff.portfolios.collectContractIds.get(StateContractId(readContractId)).fold(false) { contractPortfolio =>
                    contractIdToKeysReadingInfo(readContractId).assetBalancesReadingInfo.exists { assetBalanceReadingInfo =>
                      assetBalanceReadingInfo.assets.exists {
                        case West                 => contractPortfolio.balance != 0
                        case CustomAsset(assetId) => contractPortfolio.assets.contains(assetId)
                      }
                    }
                  }

                checkDataEntryReadingConflicts || checkContractBalanceReadingConflicts
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
      snapshotIdCheckpoint = currentSnapshotId
      processingAtomic = true
      Right(())
    }
  }

  def processAtomically(tx: Transaction, maybeCertChainWithCrl: Option[(CertChain, CrlCollection)]): Either[ValidationError, Diff] = writeLock {
    if (processingAtomic) {
      processTransaction(tx, maybeCertChainWithCrl, atomically = true)
    } else {
      Left(GenericError("Atomic transaction mining is not started"))
    }
  }

  def commitAtomic(tx: AtomicTransaction, maybeCertChainWithCrl: Option[(CertChain, CrlCollection)]): Either[ValidationError, Diff] = writeLock {
    for {
      _ <- Either.cond(processingAtomic, (), GenericError("Atomic transaction mining is not started"))
      _ = {
        diff = diffCheckpoint
        constraints = constraintsCheckpoint
        snapshots = snapshotsCheckpoint
        snapshotIdCounter.set(snapshotIdCheckpoint)
        state = CompositeBlockchainWithNG(ng, blockchain, diff)
      }
      atomicTxDiff <- processTransaction(tx, maybeCertChainWithCrl)
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
      snapshotIdCounter.set(snapshotIdCheckpoint)
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
      val newKeysReadingInfo = request.keysFilter.fold[KeysReadingInfo](AllPossibleDataEntries(request.contractId)) {
        DataEntriesByPredicate(request.contractId, _)
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
      val newKeysReadingInfo = AllPossibleDataEntries(contractId)

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
      val newKeysReadingInfo = SpecificDataEntriesSet(contractId, Set(key))

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
      val newKeysReadingInfo = SpecificDataEntriesSet(contractId, keys.toSet)

      readingContext match {
        case ContractReadingContext.Default =>
          state.contractData(contractId, keys, readingContext)
        case ContractReadingContext.TransactionExecution(txId) =>
          contractReadingWithSnapshot(txId, newKeysReadingInfo) { blockchainSnapshot =>
            blockchainSnapshot.contractData(contractId, keys, readingContext)
          }
      }
    }

  override def contractBalance(contractId: com.wavesenterprise.state.ContractId,
                               mayBeAssetId: Option[AssetId],
                               readingContext: ContractReadingContext): Long =
    readLock {
      val assetBalanceReadingInfo = SpecificAssetsBalance(contractId.byteStr, Set(mayBeAssetId.map(CustomAsset).getOrElse(West)))

      readingContext match {
        case ContractReadingContext.Default =>
          state.contractBalance(contractId, mayBeAssetId, readingContext)
        case ContractReadingContext.TransactionExecution(txId) =>
          contractReadingWithSnapshot(txId, assetBalanceReadingInfo) { blockchainSnapshot =>
            blockchainSnapshot.contractBalance(contractId, mayBeAssetId, readingContext)
          }
      }
    }

  private def contractReadingWithSnapshot[T](executableTxId: TxId, newReadingInfo: KeysReadingInfo)(readFunction: Blockchain => T): T =
    readLock {
      val newReadingDescriptor = newReadingInfo match {
        case dataKeysReadingInfo: DataEntriesReadingInfo =>
          ReadingDescriptor(currentSnapshotId, List(dataKeysReadingInfo), List.empty)
        case assetBalancesReadingInfo: SpecificAssetsBalance =>
          ReadingDescriptor(currentSnapshotId, List.empty, List(assetBalancesReadingInfo))
      }

      val ReadingDescriptor(snapshotId, _, _) = txToReadingDescriptor.compute(
        executableTxId,
        (_, maybeExistingDescriptor) =>
          Option(maybeExistingDescriptor).fold(newReadingDescriptor) { oldValue =>
            newReadingInfo match {
              case dataKeysReadingInfo: DataEntriesReadingInfo =>
                oldValue.copy(dataKeysReadingInfo = dataKeysReadingInfo :: oldValue.dataKeysReadingInfo)
              case assetBalancesReadingInfo: SpecificAssetsBalance =>
                oldValue.copy(assetBalancesReadingInfo = assetBalancesReadingInfo :: oldValue.assetBalancesReadingInfo)
            }
        }
      )

      snapshots
        .get(snapshotId)
        .fold(throw new IllegalStateException(s"Snapshot '$snapshotId' for executable transaction '$executableTxId' not found")) { snapshot =>
          readFunction(snapshot.blockchain)
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
    sealed trait DataEntriesReadingInfo extends KeysReadingInfo
    sealed trait BalanceReadingInfo     extends KeysReadingInfo

    case class AllPossibleDataEntries(contractId: ContractId)                           extends DataEntriesReadingInfo
    case class SpecificDataEntriesSet(contractId: ContractId, values: Set[String])      extends DataEntriesReadingInfo
    case class DataEntriesByPredicate(contractId: ContractId, value: String => Boolean) extends DataEntriesReadingInfo
    case class SpecificAssetsBalance(contractId: ContractId, assets: Set[Currency])     extends BalanceReadingInfo
  }

  sealed trait Currency

  object Currency {
    case object West                         extends Currency
    case class CustomAsset(assetId: ByteStr) extends Currency

    @inline
    def fromByteStrOpt(currency: Option[ByteStr]): Currency = currency match {
      case Some(assetId: AssetId) => CustomAsset(assetId)
      case None                   => West
    }

  }

  case class OptimizedReadingInfo(
      dataKeysReadingInfo: List[DataEntriesReadingInfo],
      assetBalancesReadingInfo: List[SpecificAssetsBalance]
  )

  case class ReadingDescriptor(snapshotId: SnapshotId,
                               dataKeysReadingInfo: List[DataEntriesReadingInfo],
                               assetBalancesReadingInfo: List[SpecificAssetsBalance]) {

    def optimizedKeysReadingInfoByContract: Map[ContractId, OptimizedReadingInfo] = {
      val dataKeysReadingInfoMap     = dataKeysReadingInfo.groupBy(_.contractId).withDefaultValue(List.empty)
      val assetBalanceReadingInfoMap = assetBalancesReadingInfo.groupBy(_.contractId).withDefaultValue(List.empty)

      val result = (dataKeysReadingInfoMap.keySet ++ assetBalanceReadingInfoMap.keySet).view.map { contractId =>
        optimizeContractGroup(
          contractId,
          dataKeysReadingInfoMap(contractId),
          assetBalanceReadingInfoMap(contractId)
        )
      }.toMap

      result
    }

    private def optimizeContractGroup(contractId: ContractId,
                                      keysReadingGroup: List[DataEntriesReadingInfo],
                                      assetBalanceReadingGroup: List[SpecificAssetsBalance]): (ContractId, OptimizedReadingInfo) = {
      val (allPossibleKeys, unionKeySet, maybePredicate) =
        keysReadingGroup
          .foldLeft((false, Set.empty[String], Option.empty[String => Boolean])) {
            case ((true, _, _), _) =>
              (true, Set.empty, None)
            case ((_, _, _), AllPossibleDataEntries(_)) =>
              (true, Set.empty, None)
            case ((allPossibleKeysAcc, keySetAcc, predicateAcc), SpecificDataEntriesSet(_, newSet)) =>
              (allPossibleKeysAcc, keySetAcc ++ newSet, predicateAcc)
            case ((allPossibleKeysAcc, keySetAcc, predicateAcc), DataEntriesByPredicate(_, newPredicate)) =>
              val updatedPredicateAcc = Some {
                predicateAcc.fold(newPredicate) { existingPredicate =>
                  { input =>
                    existingPredicate(input) || newPredicate(input)
                  }
                }
              }

              (allPossibleKeysAcc, keySetAcc, updatedPredicateAcc)
          }

      val optimizedDataKeysReadingInfo = List.concat(
        if (allPossibleKeys) List(AllPossibleDataEntries(contractId)) else Nil,
        List(SpecificDataEntriesSet(contractId, unionKeySet)).filter(_.values.nonEmpty),
        maybePredicate.fold(List.empty[DataEntriesReadingInfo])(predicate => List(DataEntriesByPredicate(contractId, predicate)))
      )

      val optimizedAssetBalances: List[SpecificAssetsBalance] = if (assetBalanceReadingGroup.nonEmpty) {
        List(assetBalanceReadingGroup.reduce((reading1, reading2) => reading1.copy(assets = reading1.assets ++ reading2.assets)))
      } else {
        List.empty[SpecificAssetsBalance]
      }

      contractId -> OptimizedReadingInfo(optimizedDataKeysReadingInfo, optimizedAssetBalances)
    }
  }
}

class TransactionsAccumulatorProvider(ng: NG,
                                      persistentBlockchain: Blockchain,
                                      settings: WESettings,
                                      permissionValidator: PermissionValidator,
                                      time: Time,
                                      miner: PublicKeyAccount) {

  def build(maybeUpdatedBlockchain: Option[Blockchain with MiningConstraintsHolder] = None,
            contractTxExecutor: ContractTxExecutorType = MiningExecutor): TransactionsAccumulator = {
    val initialConstraints = MiningConstraints(ng, ng.height, settings.miner.maxBlockSizeInBytes, Some(settings.miner))

    val resultMiningConstraints = maybeUpdatedBlockchain match {
      case Some(constraintsHolder) => initialConstraints.copy(total = constraintsHolder.restTotalConstraint)
      case None                    => initialConstraints
    }

    val resultBlockchain = maybeUpdatedBlockchain.getOrElse(persistentBlockchain)

    buildAccumulator(resultMiningConstraints, resultBlockchain, contractTxExecutor)
  }

  protected def buildAccumulator(resultMiningConstraints: MiningConstraints,
                                 resultBlockchain: Blockchain,
                                 contractTxExecutor: ContractTxExecutorType): TransactionsAccumulator =
    new TransactionsAccumulator(
      ng = ng,
      blockchain = resultBlockchain,
      blockchainSettings = settings.blockchain,
      permissionValidator = permissionValidator,
      time = time,
      miner = miner,
      txExpireTimeout = settings.utx.txExpireTimeout,
      miningConstraints = resultMiningConstraints,
      contractTxExecutor = contractTxExecutor
    )
}
