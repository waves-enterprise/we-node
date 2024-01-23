package com.wavesenterprise.mining

import cats.Monoid
import cats.implicits._
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.acl.PermissionValidator
import com.wavesenterprise.database.docker.KeysRequest
import com.wavesenterprise.mining.TransactionsAccumulator.KeysReadingInfo._
import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.database.rocksdb.confidential.{ConfidentialDiff, PersistentConfidentialState}
import com.wavesenterprise.mining.TransactionsAccumulator.Currency.{CustomAsset, West}
import com.wavesenterprise.settings.{BlockchainSettings, WESettings}
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.state.diffs.TransactionDiffer
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state.contracts.confidential.{CompositeConfidentialState, ConfidentialOutput, ConfidentialState}
import com.wavesenterprise.state.diffs.docker.ExecutedContractTransactionDiff.{ContractTxExecutorType, MiningExecutor, ValidatingExecutor}
import com.wavesenterprise.state.reader.{CompositeBlockchainWithNG, ReadWriteLockingBlockchain}
import com.wavesenterprise.state.{Blockchain, ByteStr, DataEntry, Diff, MiningConstraintsHolder, NG, ContractId => StateContractId}
import com.wavesenterprise.transaction.ValidationError.{CriticalConstraintOverflowError, GenericError, MvccConflictError, OneConstraintOverflowError}
import com.wavesenterprise.transaction.docker.{ExecutedContractData, ExecutedContractTransaction, ExecutedContractTransactionV5}
import com.wavesenterprise.transaction.{AssetId, AtomicTransaction, Transaction, ValidationError}
import com.wavesenterprise.utils.Time
import com.wavesenterprise.utils.pki.CrlCollection
import monix.execution.atomic.AtomicInt
import scorex.util.ScorexLogging
import com.wavesenterprise.state.ContractId

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
                              confidentialState: ConfidentialState,
                              time: Time,
                              miner: PublicKeyAccount,
                              txExpireTimeout: FiniteDuration,
                              miningConstraint: MiningConstraint,
                              contractTxExecutor: ContractTxExecutorType = MiningExecutor)
    extends ReadWriteLockingBlockchain
    with ScorexLogging {

  import TransactionsAccumulator._

  private[this] val snapshotIdCounter = AtomicInt(0)

  @inline
  private def currentSnapshotId: SnapshotId = snapshotIdCounter.get()

  @inline
  private def nextSnapshotId(): SnapshotId = snapshotIdCounter.incrementAndGet()

  protected[this] var state: Blockchain = blockchain
  private[this] var diff                = Diff.empty
  private[this] var constraints         = miningConstraint

  private[this] var snapshots                      = SortedMap(currentSnapshotId -> Snapshot(diff, state))
  private[this] var confidentialSnapshot           = ConfidentialSnapshot(Monoid.empty[ConfidentialDiff], confidentialState)
  private[this] var confidentialSnapshotCheckpoint = confidentialSnapshot

  private[this] var diffCheckpoint        = diff
  private[this] var constraintsCheckpoint = constraints
  private[this] var snapshotsCheckpoint   = snapshots
  private[this] var snapshotIdCheckpoint  = currentSnapshotId

  private[this] val txToReadingDescriptor = new ConcurrentHashMap[TxIdByteStr, ReadingDescriptor]

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

  def process(tx: Transaction,
              confidentialOutputs: Seq[ConfidentialOutput],
              maybeCertChainWithCrl: Option[(CertChain, CrlCollection)]): Either[ValidationError, Diff] = writeLock {
    if (processingAtomic) {
      Left(GenericError("Can't process transaction during atomic transaction mining"))
    } else {
      processTransaction(tx, confidentialOutputs, maybeCertChainWithCrl)
    }
  }

  private def processTransaction(tx: Transaction,
                                 confidentialOutputs: Seq[ConfidentialOutput],
                                 maybeCertChainWithCrl: Option[(CertChain, CrlCollection)],
                                 atomically: Boolean = false): Either[ValidationError, Diff] = {
    val conflictFound = tx match {
      case err: ExecutedContractTransactionV5 if err.statusCode != 0 => false
      case _                                                         => findConflicts(tx)
    }
    lazy val updatedConstraints = constraints.put(state, tx)

    if (conflictFound) {
      Left(MvccConflictError)
    } else if (updatedConstraints.isOverfilled) {
      updatedConstraints match {
        case miningConstraint: MiningConstraint if miningConstraint.hasOverfilledCriticalConstraint =>
          Left(CriticalConstraintOverflowError)
        case _ => Left(OneConstraintOverflowError)
      }
    } else {
      txDiffer(state, tx, maybeCertChainWithCrl, atomically).map { txDiff =>
        confidentialOutputs.foreach { confidentialOutput =>
          val confidentialDiff = ConfidentialDiff.fromOutput(confidentialOutput)
          val newDiff          = confidentialSnapshot.diff combine confidentialDiff
          val newState         = CompositeConfidentialState.composite(confidentialSnapshot.state, newDiff)
          confidentialSnapshot = ConfidentialSnapshot(newDiff, newState)
        }

        constraints = updatedConstraints
        diff = diff combine txDiff
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
      confidentialSnapshotCheckpoint = confidentialSnapshot
      diffCheckpoint = diff
      constraintsCheckpoint = constraints
      snapshotsCheckpoint = snapshots
      snapshotIdCheckpoint = currentSnapshotId
      processingAtomic = true
      Right(())
    }
  }

  def processAtomically(tx: Transaction,
                        confidentialOutput: Seq[ConfidentialOutput],
                        maybeCertChainWithCrl: Option[(CertChain, CrlCollection)]): Either[ValidationError, Diff] = writeLock {
    if (processingAtomic) {
      processTransaction(tx, confidentialOutput, maybeCertChainWithCrl, atomically = true)
    } else {
      Left(GenericError("Atomic transaction mining is not started"))
    }
  }

  def commitAtomic(tx: AtomicTransaction,
                   confidentialOutputs: Seq[ConfidentialOutput],
                   maybeCertChainWithCrl: Option[(CertChain, CrlCollection)]): Either[ValidationError, Diff] = writeLock {
    for {
      _ <- Either.cond(processingAtomic, (), GenericError("Atomic transaction mining is not started"))
      _ = {
        diff = diffCheckpoint
        constraints = constraintsCheckpoint
        snapshots = snapshotsCheckpoint
        snapshotIdCounter.set(snapshotIdCheckpoint)
        state = CompositeBlockchainWithNG(ng, blockchain, diff)
        confidentialSnapshot = confidentialSnapshotCheckpoint
      }
      atomicTxDiff <- processTransaction(tx, confidentialOutputs, maybeCertChainWithCrl)
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
      confidentialSnapshot = confidentialSnapshotCheckpoint
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
      val wrappedContractId = ContractId(request.contractId)
      if (contractIsConfidential(wrappedContractId)) {
        confidentialSnapshot.state.contractKeys(request)
      } else {
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
  }

  override def contractData(contractId: ContractIdByteStr, readingContext: ContractReadingContext): ExecutedContractData =
    readLock {
      val wrappedContractId = ContractId(contractId)
      if (contractIsConfidential(wrappedContractId)) {
        confidentialSnapshot.state.contractData(wrappedContractId)
      } else {
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
    }

  override def contractData(contractId: ContractIdByteStr, key: String, readingContext: ContractReadingContext): Option[DataEntry[_]] =
    readLock {
      val wrappedContractId = ContractId(contractId)
      if (contractIsConfidential(wrappedContractId)) {
        confidentialSnapshot.state.contractData(wrappedContractId, key)
      } else {
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
    }

  override def contractData(contractId: ContractIdByteStr, keys: Iterable[String], readingContext: ContractReadingContext): ExecutedContractData =
    readLock {
      val wrappedContractId = ContractId(contractId)
      if (contractIsConfidential(wrappedContractId)) {
        confidentialSnapshot.state.contractData(wrappedContractId, keys)
      } else {
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
    }

  override def contractBalance(contractId: ContractId, mayBeAssetId: Option[AssetId], readingContext: ContractReadingContext): Long =
    readLock {
      if (contractIsConfidential(contractId)) {
        throw new NotImplementedError("Confidential smart contract doesn't support the functionality of native tokens")
      } else {
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
    }

  @inline
  private def contractIsConfidential(contractId: ContractId): Boolean =
    state.contract(contractId).exists(_.isConfidential)

  private def contractReadingWithSnapshot[T](executableTxId: TxIdByteStr, newReadingInfo: KeysReadingInfo)(readFunction: Blockchain => T): T =
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
  private type SnapshotId        = Int
  private type TxIdByteStr       = ByteStr
  private type ContractIdByteStr = ByteStr

  private case class Snapshot(diff: Diff, blockchain: Blockchain)
  private case class ConfidentialSnapshot(diff: ConfidentialDiff, state: ConfidentialState)

  sealed trait KeysReadingInfo {
    def contractId: ContractIdByteStr
  }

  object KeysReadingInfo {
    sealed trait DataEntriesReadingInfo extends KeysReadingInfo
    sealed trait BalanceReadingInfo     extends KeysReadingInfo

    case class AllPossibleDataEntries(contractId: ContractIdByteStr)                           extends DataEntriesReadingInfo
    case class SpecificDataEntriesSet(contractId: ContractIdByteStr, values: Set[String])      extends DataEntriesReadingInfo
    case class DataEntriesByPredicate(contractId: ContractIdByteStr, value: String => Boolean) extends DataEntriesReadingInfo
    case class SpecificAssetsBalance(contractId: ContractIdByteStr, assets: Set[Currency])     extends BalanceReadingInfo
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

    def optimizedKeysReadingInfoByContract: Map[ContractIdByteStr, OptimizedReadingInfo] = {
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

    private def optimizeContractGroup(contractId: ContractIdByteStr,
                                      keysReadingGroup: List[DataEntriesReadingInfo],
                                      assetBalanceReadingGroup: List[SpecificAssetsBalance]): (ContractIdByteStr, OptimizedReadingInfo) = {
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
                predicateAcc.fold(newPredicate) {
                  existingPredicate =>
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
                                      persistentConfidentialState: PersistentConfidentialState,
                                      settings: WESettings,
                                      permissionValidator: PermissionValidator,
                                      time: Time,
                                      miner: PublicKeyAccount) {

  def build(maybeUpdatedBlockchain: Option[Blockchain with MiningConstraintsHolder] = None,
            maybeUpdatedConfidentialState: Option[PersistentConfidentialState] = None,
            contractTxExecutor: ContractTxExecutorType = MiningExecutor): TransactionsAccumulator = {
    val initialConstraints = MiningConstraints(ng, ng.height, settings.miner.maxBlockSizeInBytes, Some(settings.miner))

    val resultMiningConstraints = maybeUpdatedBlockchain match {
      case Some(constraintsHolder) => initialConstraints.copy(total = constraintsHolder.restTotalConstraint)
      case None                    => initialConstraints
    }

    val resultBlockchain        = maybeUpdatedBlockchain.getOrElse(persistentBlockchain)
    val resultConfidentialState = maybeUpdatedConfidentialState.getOrElse(persistentConfidentialState)

    buildAccumulator(resultMiningConstraints, resultBlockchain, resultConfidentialState, contractTxExecutor)
  }

  protected def buildAccumulator(resultMiningConstraints: MiningConstraints,
                                 resultBlockchain: Blockchain,
                                 resultConfidentialState: ConfidentialState,
                                 contractTxExecutor: ContractTxExecutorType): TransactionsAccumulator = {
    val miningConstraint = contractTxExecutor match {
      case MiningExecutor     => resultMiningConstraints.total
      case ValidatingExecutor => resultMiningConstraints.micro
    }
    new TransactionsAccumulator(
      ng = ng,
      blockchain = resultBlockchain,
      blockchainSettings = settings.blockchain,
      permissionValidator = permissionValidator,
      confidentialState = resultConfidentialState,
      time = time,
      miner = miner,
      txExpireTimeout = settings.utx.txExpireTimeout,
      miningConstraint = miningConstraint,
      contractTxExecutor = contractTxExecutor
    )
  }
}
