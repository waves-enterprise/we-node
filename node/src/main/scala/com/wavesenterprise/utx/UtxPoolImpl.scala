package com.wavesenterprise.utx

import cats._
import cats.implicits._
import com.wavesenterprise.account.{Address, PublicKeyAccount}
import com.wavesenterprise.acl.PermissionValidator
import com.wavesenterprise.certs.CertChain
import com.wavesenterprise.database.snapshot.{ConsensualSnapshotSettings, EnabledSnapshot}
import com.wavesenterprise.metrics.Instrumented
import com.wavesenterprise.network.{TransactionWithSize, TxBroadcaster}
import com.wavesenterprise.protobuf.service.transaction.UtxSize
import com.wavesenterprise.settings.{BlockchainSettings, UtxSettings}
import com.wavesenterprise.state.diffs.TransactionDiffer
import com.wavesenterprise.state.{Blockchain, ByteStr, Diff, Portfolio}
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.assets.ReissueTransaction
import com.wavesenterprise.transaction.docker.ExecutedContractTransaction
import com.wavesenterprise.transaction.validation.ExecutableValidation
import com.wavesenterprise.utils._
import com.wavesenterprise.utils.pki.CrlCollection
import kamon.Kamon
import kamon.metric.MeasurementUnit
import monix.eval.Task
import monix.execution.atomic.{AtomicInt, AtomicLong}
import monix.execution.{CancelableFuture, Scheduler}
import monix.reactive.OverflowStrategy
import monix.reactive.subjects.ConcurrentSubject
import org.reactivestreams.Publisher
import play.api.libs.json.Json

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationLong
import scala.util.{Left, Right}

class UtxPoolImpl(time: Time,
                  blockchain: Blockchain with BlockchainUpdater,
                  blockchainSettings: BlockchainSettings,
                  utxSettings: UtxSettings,
                  permissionValidator: PermissionValidator,
                  utxPoolSyncScheduler: Scheduler,
                  snapshotSettings: ConsensualSnapshotSettings,
                  txBroadcaster: => TxBroadcaster,
)(implicit val utxBackgroundScheduler: Scheduler)
    extends UtxPool
    with NopeUtxCertStorage
    with ScorexLogging
    with Instrumented
    with AutoCloseable {

  import com.wavesenterprise.utx.UtxPool._
  import com.wavesenterprise.utx.UtxPoolImpl._

  private[this] val transactions          = new ConcurrentHashMap[ByteStr, Transaction]().asScala
  private[this] val atomicInnerTxIds      = ConcurrentHashMap.newKeySet[ByteStr]().asScala
  private[this] val sizeInfo              = new SizeCounter(utxSettings.memoryLimit.toBytes.toLong)
  private[this] val pessimisticPortfolios = new PessimisticPortfolios

  @volatile
  private[this] var snapshotStatusValidation = ().asRight[ValidationError]

  private val snapshotStatusValidationProcess = blockchain.lastBlockInfo
    .asyncBoundary(OverflowStrategy.Default)
    .doOnNext { blockInfo =>
      Task {
        snapshotStatusValidation = snapshotSettings match {
          case enabledSnapshot: EnabledSnapshot =>
            def left: ValidationError = {
              log.info(s"Snapshot height '${enabledSnapshot.snapshotHeight.value}' is reached, locking UTX pool.")
              ValidationError.ReachedSnapshotHeightError(enabledSnapshot.snapshotHeight.value)
            }

            Either.cond(blockInfo.height < enabledSnapshot.snapshotHeight.value, (), left)
          case _ =>
            Right(())
        }
      }
    }
    .logErr
    .onErrorRestartUnlimited
    .subscribe()

  private[this] val cleanupTask: Task[Unit] =
    Task(cleanup()) >>
      Task.sleep(utxSettings.cleanupInterval) >>
      cleanupTask

  private val cleanupProcess: CancelableFuture[Unit] = cleanupTask.runAsyncLogErr

  private[this] val rebroadcastTask: Task[Unit] =
    Task.defer(rebroadcastOldTxs()) >>
      Task.sleep(utxSettings.rebroadcastInterval) >>
      rebroadcastTask

  private val rebroadcastProcess: CancelableFuture[Unit] = rebroadcastTask.runAsyncLogErr

  def rebroadcastOldTxs(): Task[Unit] = {
    val currentTime                = time.getTimestamp()
    val rebroadcastThresholdMillis = utxSettings.rebroadcastThreshold.toMillis
    val broadcastTasks = transactions.map {
      case (txId, tx) =>
        val txLiveTime      = currentTime - tx.timestamp
        val needRebroadcast = txLiveTime >= rebroadcastThresholdMillis

        if (needRebroadcast) {
          log.trace(s"Rebroadcast old tx: '$txId', time elapsed from tx timestamp: ${humanReadableDuration(txLiveTime)}")
          txBroadcaster
            .forceBroadcast(tx, getCertChain(txId))
            .value
            .map(_ => ())
        } else {
          Task.unit
        }
    }

    Task.parSequenceUnordered(broadcastTasks).map(_ => ())
  }

  private val innerLastSize        = ConcurrentSubject.behavior[UtxSize](size, OverflowStrategy.DropOld(2))(utxPoolSyncScheduler)
  val lastSize: Publisher[UtxSize] = innerLastSize.throttleLast(1.second).toReactivePublisher(utxPoolSyncScheduler)

  def cleanup(): Unit = {
    removeInvalid()
    removeExpired()
  }

  def close(): Unit = {
    cleanupProcess.cancel()
    rebroadcastProcess.cancel()
    snapshotStatusValidationProcess.cancel()
  }

  private[this] val utxPoolSizeStats          = Kamon.rangeSampler("utx-pool-size", MeasurementUnit.none, Duration.of(500, ChronoUnit.MILLIS))
  private[this] val processingTimeStats       = Kamon.histogram("utx-transaction-processing-time", MeasurementUnit.time.milliseconds)
  private[this] val successfulPutRequestStats = Kamon.counter("utx-pool-failed-put")
  private[this] val failedPutRequestStats     = Kamon.counter("utx-pool-successful-put")
  private[this] val utxPoolSizeInBytesStats =
    Kamon.rangeSampler("utx-pool-size-in-bytes", MeasurementUnit.information.bytes, Duration.of(500, ChronoUnit.MILLIS))

  private[this] def removeExpired(): Unit = {
    val currentTs = time.correctedTime()
    def isExpired(tx: Transaction) =
      (currentTs - tx.timestamp).millis > utxSettings.txExpireTimeout

    transactions.values
      .filter(isExpired)
      .foreach(remove(_, Some("Expired")))
  }

  def removeAll(txs: Seq[Transaction], mustBeInPool: Boolean): Unit = {
    txs.foreach(tx => remove(tx, None, mustBeInPool))
  }

  def removeAll(txToError: Map[Transaction, String]): Unit = {
    txToError.foreach { case (tx, reason) => remove(tx, Some(reason)) }
  }

  def remove(tx: Transaction, reason: Option[String], mustBeInPool: Boolean = true): Unit =
    tx match {
      case transaction: ExecutedContractTransaction =>
        removeById(transaction.tx.id(), reason, mustBeInPool)
      case atomic: AtomicTransaction =>
        removeById(atomic.id(), reason, mustBeInPool)
        atomic.transactions.foreach(tx => atomicInnerTxIds.remove(tx.id()))
      case transaction =>
        removeById(transaction.id(), reason, mustBeInPool)
    }

  private[this] def removeById(txId: ByteStr, reason: Option[String], mustBeInPool: Boolean): Unit = {
    val maybeRemovedTx = transactions.remove(txId)

    maybeRemovedTx match {
      case Some(tx) =>
        reason.foreach { message =>
          if (log.logger.isTraceEnabled) {
            log.trace(s"The following transaction has been removed from UTX. Reason: '$message'. '${tx.toString}''")
          } else {
            log.debug(s"Transaction '$txId' has been removed from UTX. Reason: '$message'")
          }
        }
        utxPoolSizeStats.decrement()
        utxPoolSizeInBytesStats.decrement(tx.bytes().length)
      case None if mustBeInPool =>
        log.warn(s"Trying to remove non-existent transaction '$txId' from UTX")
      case None => ()
    }

    maybeRemovedTx.foreach(removeCertChain)
    sizeInfo.remove(txId)
    pessimisticPortfolios.remove(txId)
    innerLastSize.onNext(size)
  }

  protected def buildTransactionDiffer: TransactionDiffer =
    TransactionDiffer(
      settings = blockchainSettings,
      permissionValidator = permissionValidator,
      prevBlockTimestamp = blockchain.lastPersistenceBlock.map(_.timestamp),
      currentBlockTimestamp = time.correctedTime(),
      currentBlockHeight = blockchain.height,
      txExpireTimeout = utxSettings.txExpireTimeout
    )

  private[this] def removeInvalid(): Unit = {
    val txDiffer = buildTransactionDiffer
    val transactionsToRemove = transactions.values
      .map { tx =>
        val maybeCertChain         = getCertChain(tx.id())
        val maybeCertChainWithCrls = maybeCertChain.flatMap(findCrlsForCertChain(_, time.correctedTime()))
        tx -> txDiffer(blockchain, tx, maybeCertChainWithCrls)
      }
      .collect {
        case (t, Left(error)) => (t, error.toString)
      }
      .toMap
    removeAll(transactionsToRemove)
  }

  protected def findCrlsForCertChain(certChain: CertChain, timestamp: Long): Option[(CertChain, CrlCollection)] = None

  def accountPortfolio(addr: Address): Portfolio = blockchain.addressPortfolio(addr)

  def portfolio(addr: Address): Portfolio =
    Monoid.combine(blockchain.addressPortfolio(addr), pessimisticPortfolios.getAggregated(addr))

  def all: Seq[Transaction] = transactions.values.toSeq.sorted(Transaction.timestampOrdering)

  def transactionById(transactionId: ByteStr): Option[Transaction] = transactions.get(transactionId)

  def transactionWithCertsById(transactionId: ByteStr): Option[TxWithCerts] = {
    val maybeTx    = transactions.get(transactionId)
    val maybeCerts = getCertChain(transactionId)

    maybeTx.map(TxWithCerts(_, maybeCerts))
  }

  private def canReissue(tx: Transaction): Either[GenericError, Unit] = tx match {
    case r: ReissueTransaction if blockchain.assetDescription(r.assetId).exists(!_.reissuable) => Left(GenericError(s"Asset is not reissuable"))
    case _                                                                                     => Right(())
  }

  private def checkAlias(tx: Transaction): Either[GenericError, Unit] = tx match {
    case cat: CreateAliasTransaction if !blockchain.canCreateAlias(cat.alias) => Left(GenericError("Alias already claimed"))
    case _                                                                    => Right(())
  }

  private def checkScripted(tx: Transaction): Either[GenericError, Unit] =
    tx match {
      case a: AuthorizedTransaction if blockchain.hasScript(a.sender.toAddress) && (!utxSettings.allowTransactionsFromSmartAccounts) =>
        Left(GenericError("transactions from scripted accounts are denied from UTX pool"))
      case _ => Right(())
    }

  override def putIfNew(tx: Transaction, maybeCerts: Option[CertChain]): Either[ValidationError, (Boolean, Diff)] = {
    putIfNewWithSize(TransactionWithSize(tx.bytes().length, tx), maybeCerts)
  }

  override def txDiffer(tx: Transaction, maybeCertChain: Option[CertChain]): Either[ValidationError, Diff] =
    for {
      _    <- sizeInfo.checkFits(TransactionWithSize(tx.bytes().length, tx))
      diff <- validateAndDiffer(tx, maybeCertChain).leftMap(err => { log.trace(s"UTX putIfNew(${tx.id()}) failed with $err"); err })
    } yield diff

  override def putIfNewWithSize(txMessage: TransactionWithSize, maybeCertChain: Option[CertChain]): Either[ValidationError, (Boolean, Diff)] = {
    for {
      _ <- snapshotStatusValidation
      _ <- sizeInfo.checkFits(txMessage)
      r <- validateAndPut(txMessage, maybeCertChain)
      _ = innerLastSize.onNext(size)
    } yield r
  }

  private def validateAndPut(txWithSize: TransactionWithSize, maybeCertChain: Option[CertChain]): Either[ValidationError, (Boolean, Diff)] = {
    val result = measureSuccessful(
      processingTimeStats,
      validateAndDiffer(txWithSize.tx, maybeCertChain).map { diff =>
        tryToPut(txWithSize, diff, maybeCertChain) -> diff
      }
    )

    result.fold(
      err => {
        failedPutRequestStats.increment()
        log.trace(s"UTX putIfNew(${txWithSize.tx.id()}) failed with $err")
      },
      r => {
        if (r._1) {
          successfulPutRequestStats.increment()
        }
        log.trace(s"UTX putIfNew(${txWithSize.tx.id()}) succeeded, isNew = ${r._1}")
      }
    )

    result
  }

  protected def validateAndDiffer(tx: Transaction,
                                  maybeCertChain: Option[CertChain],
                                  maybePk: Option[PublicKeyAccount] = None): Either[ValidationError, Diff] = {
    for {
      _ <- ExecutableValidation.validateApiVersion(tx, blockchain)
      _ <- checkScripted(tx)
      _ <- checkAlias(tx)
      _ <- canReissue(tx)
      differ = buildTransactionDiffer
      maybePk <- encodedPubKeyFromTx(tx)
      currentTime            = time.correctedTime()
      maybeCertChainWithCrls = filterProvidedCertChain(maybePk, maybeCertChain).flatMap(findCrlsForCertChain(_, currentTime))
      diff <- differ(blockchain, tx, maybeCertChainWithCrls)
    } yield diff
  }

  protected def encodedPubKeyFromTx(tx: Transaction): Either[ValidationError, Option[PublicKeyAccount]] = Right(None)

  private def filterProvidedCertChain(maybePk: Option[PublicKeyAccount], certChain: Option[CertChain]): Option[CertChain] = {
    maybePk match {
      case Some(pk) => certChain.filter(_ => blockchain.certByPublicKey(pk).isEmpty)
      case _        => certChain
    }
  }

  private def tryToPut(txWithSize: TransactionWithSize, diff: Diff, maybeCerts: Option[CertChain]): Boolean = {
    pessimisticPortfolios.add(txWithSize.tx.id(), diff)
    val isNew = transactions.put(txWithSize.tx.id(), txWithSize.tx).isEmpty

    if (isNew) {
      txWithSize.tx match {
        case atomic: AtomicTransaction => atomic.transactions.foreach(tx => atomicInnerTxIds.add(tx.id()))
        case _                         => ()
      }

      maybeCerts.foreach(putCertChain(txWithSize.tx, _))
      utxPoolSizeStats.increment()
      utxPoolSizeInBytesStats.increment(txWithSize.size)
      sizeInfo.put(txWithSize)
    }

    log.trace(s"putIfNew for tx:\n${txWithSize.tx.json.map(Json.prettyPrint).value()}\nisNew = $isNew, diff = $diff)}")

    isNew
  }

  @inline
  override def selectTransactions(predicate: Transaction => Boolean): Array[Transaction] =
    transactions.values.filter(predicate).toArray

  override def selectOrderedTransactions(predicate: Transaction => Boolean): Array[Transaction] = {
    val transactions = selectTransactions(predicate)
    // Using java sorting so as not to create a copy of the collection. Performance is important here.
    java.util.Arrays.sort(transactions, Transaction.timestampOrdering[Transaction])
    transactions
  }

  override def selectTransactionsWithCerts(predicate: Transaction => Boolean): Array[TxWithCerts] =
    selectTransactions(predicate).map(zipTxWithCert)

  override def selectOrderedTransactionsWithCerts(predicate: Transaction => Boolean): Array[TxWithCerts] =
    selectOrderedTransactions(predicate).map(zipTxWithCert)

  private def zipTxWithCert(tx: Transaction): TxWithCerts = TxWithCerts(tx, getCertChain(tx.id()))

  override def forcePut(txWithSize: TransactionWithSize, diff: Diff, maybeCerts: Option[CertChain]): Boolean = {
    val added = tryToPut(txWithSize, diff, maybeCerts)
    if (added) {
      successfulPutRequestStats.increment()
    }
    log.trace(s"UTX putIfNew(${txWithSize.tx.id()}) succeeded, isNew = $added")
    added
  }

  override def size: UtxSize = sizeInfo.utxSize

  override def contains(transactionId: ByteStr): Boolean = transactions.contains(transactionId)

  override def containsInsideAtomic(transactionId: ByteStr): Boolean = atomicInnerTxIds.contains(transactionId)
}

object UtxPoolImpl {

  private class PessimisticPortfolios {
    private type Portfolios = Map[Address, Portfolio]
    private val transactionPortfolios = new ConcurrentHashMap[ByteStr, Portfolios]().asScala
    private val transactions          = new ConcurrentHashMap[Address, Set[ByteStr]]().asScala

    def add(txId: ByteStr, txDiff: Diff): Unit = {
      val nonEmptyPessimisticPortfolios = txDiff.portfolios.collectAddresses
        .map {
          case (addr, portfolio) => addr -> portfolio.pessimistic
        }
        .filterNot {
          case (_, portfolio) => portfolio.isEmpty
        }

      if (nonEmptyPessimisticPortfolios.nonEmpty &&
          transactionPortfolios.put(txId, nonEmptyPessimisticPortfolios).isEmpty) {
        nonEmptyPessimisticPortfolios.keys.foreach { address =>
          transactions.put(address, transactions.getOrElse(address, Set.empty) + txId)
        }
      }
    }

    def getAggregated(accountAddr: Address): Portfolio = {
      val portfolios = for {
        txId <- transactions.getOrElse(accountAddr, Set.empty).toSeq
        txPortfolios = transactionPortfolios.getOrElse(txId, Map.empty[Address, Portfolio])
        txAccountPortfolio <- txPortfolios.get(accountAddr).toSeq
      } yield txAccountPortfolio

      Monoid.combineAll[Portfolio](portfolios)
    }

    def remove(txId: ByteStr): Unit = {
      if (transactionPortfolios.remove(txId).isDefined) {
        transactions.keySet.foreach { addr =>
          transactions.put(addr, transactions.getOrElse(addr, Set.empty) - txId)
        }
      }
    }
  }

  private final class SizeCounter(limit: Long) {

    private[this] val elementsCount = AtomicInt(0)
    private[this] val sizesSum      = AtomicLong(0L)
    private[this] val txToSize      = new ConcurrentHashMap[ByteStr, Int]

    def utxSize: UtxSize = UtxSize(elementsCount.get, sizesSum.get)

    def put(txWithSize: TransactionWithSize): Unit =
      Option(txToSize.put(txWithSize.tx.id(), txWithSize.size)) match {
        case None =>
          sizesSum.increment(txWithSize.size)
          elementsCount.increment()
        case _ => ()
      }

    def remove(id: ByteStr): Unit =
      Option(txToSize.remove(id))
        .foreach { size =>
          sizesSum.decrement(size)
          elementsCount.decrement()
        }

    def checkFits(txWithSize: TransactionWithSize): Either[GenericError, Unit] =
      if (fits(txWithSize)) Right(()) else Left(GenericError("Transaction pool bytes size limit is reached"))

    @inline
    private def fits(txWithSize: TransactionWithSize): Boolean =
      txToSize.contains(txWithSize.tx.id()) || (sizesSum.get + txWithSize.size) <= limit
  }
}
