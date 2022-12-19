package com.wavesenterprise.network

import cats.data.EitherT
import cats.implicits._
import com.google.common.cache.CacheBuilder
import com.wavesenterprise.account.Address
import com.wavesenterprise.certs.{CertChain, CertChainStore}
import com.wavesenterprise.consensus.Consensus
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.internals.CryptoError
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.peers.MinersFirstWriter.WriteResult
import com.wavesenterprise.settings.SynchronizationSettings.TxBroadcasterSettings
import com.wavesenterprise.settings.{NodeMode, WESettings}
import com.wavesenterprise.state.diffs.TransactionDiffer.TransactionValidationError
import com.wavesenterprise.state.{Blockchain, ByteStr, Diff}
import com.wavesenterprise.transaction.ValidationError.{AlreadyInProcessing, AlreadyInTheState, UnsupportedContractApiVersion}
import com.wavesenterprise.transaction.{Transaction, ValidationError}
import com.wavesenterprise.utils.ScorexLogging
import com.wavesenterprise.utx.UtxPool
import io.netty.channel.Channel
import io.netty.channel.group.ChannelMatcher
import monix.eval.Task
import monix.execution.{Cancelable, Scheduler}
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{Observable, OverflowStrategy}

import java.util.concurrent.TimeUnit
import scala.util.control.NoStackTrace

sealed trait TxBroadcaster extends AutoCloseable {
  def utx: UtxPool
  def activePeerConnections: ActivePeerConnections
  def txDiffer(tx: Transaction, certChain: Option[CertChain] = None): Either[ValidationError, Diff]
  def broadcast(txWithSize: TransactionWithSize, diff: Diff, certChain: Option[CertChain]): EitherT[Task, CryptoError, Unit]
  def forceBroadcast(tx: Transaction, certChain: Option[CertChain]): EitherT[Task, CryptoError, Unit]
  def broadcastIfNew(tx: Transaction, certChain: Option[CertChain] = None): EitherT[Task, ValidationError, Transaction]
  def broadcastBatchIfNew(txs: Seq[TxFromChannel]): Task[Unit]
}

object TxBroadcaster {
  def apply(settings: WESettings,
            blockchain: Blockchain,
            consensus: Consensus,
            utx: UtxPool,
            activePeerConnections: ActivePeerConnections,
            maxSimultaneousConnections: Int)(implicit scheduler: Scheduler): TxBroadcaster = {
    settings.network.mode match {
      case NodeMode.Default =>
        new EnabledTxBroadcaster(settings.synchronization.transactionBroadcaster,
                                 blockchain,
                                 consensus,
                                 utx,
                                 activePeerConnections,
                                 maxSimultaneousConnections)
      case _ =>
        DisabledTxBroadcaster(utx, activePeerConnections)
    }
  }
}

case class DisabledTxBroadcaster(utx: UtxPool, activePeerConnections: ActivePeerConnections) extends TxBroadcaster {
  override def txDiffer(tx: Transaction, certChain: Option[CertChain] = None): Either[ValidationError, Diff] = utx.txDiffer(tx, certChain)

  override def broadcast(txWithSize: TransactionWithSize, diff: Diff, certChain: Option[CertChain]): EitherT[Task, CryptoError, Unit] =
    EitherT.rightT(())

  override def broadcastIfNew(tx: Transaction, certChain: Option[CertChain]): EitherT[Task, ValidationError, Transaction] =
    EitherT.rightT(tx)

  override def forceBroadcast(tx: Transaction, certChain: Option[CertChain]): EitherT[Task, CryptoError, Unit] = EitherT.rightT(())

  override def broadcastBatchIfNew(txs: Seq[TxFromChannel]): Task[Unit] = Task.unit

  override def close(): Unit = {}
}

class EnabledTxBroadcaster(
    txBroadcasterSettings: TxBroadcasterSettings,
    val blockchain: Blockchain,
    val consensus: Consensus,
    val utx: UtxPool,
    val activePeerConnections: ActivePeerConnections,
    maxSimultaneousConnections: Int
)(implicit val scheduler: Scheduler)
    extends TxBroadcaster
    with ScorexLogging {
  import EnabledTxBroadcaster._

  @volatile private[this] var cachedCurrentAndNextMiners = InitCurrentAndNextRoundMiners

  private[this] val knownTransactions = CacheBuilder
    .newBuilder()
    .maximumSize(txBroadcasterSettings.knownTxCacheSize)
    .expireAfterWrite(txBroadcasterSettings.knownTxCacheTime.toMillis, TimeUnit.MILLISECONDS)
    .build[ByteStr, Object]

  private[this] val taskQueue = ConcurrentSubject.publish[BroadcastTask](OverflowStrategy.Unbounded)

  private[this] val taskProcessing: Cancelable = taskQueue
    .bufferTimedAndCounted(txBroadcasterSettings.maxBatchTime, txBroadcasterSettings.maxBatchSize)
    .mapParallelUnordered(Runtime.getRuntime.availableProcessors()) { batch =>
      if (batch.nonEmpty) {
        for {
          successBroadcasts <- runBroadcasts(batch)
          _                 <- processBroadcastsResult(batch, successBroadcasts)
        } yield ()
      } else {
        Task.unit
      }
    }
    .logErr
    .onErrorRestartUnlimited
    .doOnComplete(Task(log.info("TxBroadcaster stops")))
    .subscribe()

  private[this] val currentMinersUpdater: Cancelable = Observable
    .interval(txBroadcasterSettings.minersUpdateInterval)
    .map { _ =>
      val currentHeight = blockchain.height
      if (currentHeight > cachedCurrentAndNextMiners.height) {
        val newCurrentAndNextMiners = consensus
          .getCurrentAndNextMiners()
          .leftMap { err =>
            log.warn(s"Failed to get current and next round miners when trying to send them new tx first, error: $err")
          }
          .getOrElse(Seq())

        cachedCurrentAndNextMiners = CurrentAndNextMiners(currentHeight, newCurrentAndNextMiners)
      }
    }
    .logErr
    .onErrorRestartUnlimited
    .subscribe()

  private def runBroadcasts(batch: Seq[BroadcastTask]): Task[List[SuccessBroadcast]] =
    if (activePeerConnections.isEmpty) {
      Task(log.warn(s"Broadcast failed: empty connected peers")).as(List.empty)
    } else {
      (Task.parSequenceUnordered(batch.map(broadcastAttempt)) <* Task(activePeerConnections.flushWrites()))
        .flatMap(processWriteResults)
    }

  private def processWriteResults(writeResults: List[TxWriteResult]): Task[List[SuccessBroadcast]] =
    Task
      .parSequenceUnordered {
        writeResults.map {
          case TxWriteResult(txId, writeResult) =>
            writeResult.allSuccessChannels
              .map { successChannels =>
                TxAwaitedResult(txId, successChannels)
              }
        }
      }
      .flatMap { awaitedResults =>
        val broadcastsResult = awaitedResults.map {
          case TxAwaitedResult(txId, allSuccessChannels) =>
            Either.cond(
              allSuccessChannels.nonEmpty,
              SuccessBroadcast(txId, allSuccessChannels),
              new RuntimeException(s"Failed to broadcast tx '$txId': empty successful set") with NoStackTrace
            )
        }

        val (failed, success) = broadcastsResult.separate

        Task(log.debug(s"Broadcast results: success count '${success.size}', failed count '${failed.size}'")) *>
          Task(logFailedBroadcasts(failed)).as(success)
      }

  private def logFailedBroadcasts(failures: Seq[Throwable]): Unit =
    failures.foreach { ex =>
      log.debug("Broadcast error", ex)
    }

  private def broadcastAttempt(task: BroadcastTask): Task[TxWriteResult] =
    Task.defer {
      val channelMatcher: ChannelMatcher = { channel: Channel =>
        !task.exceptChannels.contains(channel) && !ActivePeerConnections.channelIsWatcher(channel)
      }

      if (!activePeerConnections.isThereAnyConnection(channelMatcher)) {
        Task.raiseError(new RuntimeException("No one to broadcast") with NoStackTrace)
      } else {
        Task {
          val broadcastedTx = BroadcastedTransaction(TransactionWithSize(task.tx.bytes().length, task.tx), task.certStore)
          val writeResult   = writeToGroup(broadcastedTx, channelMatcher)

          TxWriteResult(task.tx.id(), writeResult)
        }
      }
    }

  private def writeToGroup(message: BroadcastedTransaction, channelMatcher: ChannelMatcher): WriteResult = {
    val currentAndNextMiners = cachedCurrentAndNextMiners.miners

    if (txBroadcasterSettings.maxBroadcastCount < maxSimultaneousConnections) {
      activePeerConnections.writeToRandomSubGroupMinersFirst(message, currentAndNextMiners, channelMatcher, txBroadcasterSettings.maxBroadcastCount)
    } else {
      activePeerConnections.writeMsgMinersFirst(message, currentAndNextMiners, channelMatcher)
    }
  }

  private def processBroadcastsResult(sentBatch: Seq[BroadcastTask], successBroadcasts: Seq[SuccessBroadcast]): Task[Unit] =
    Task.defer {
      val txIdToBroadcastChannels = successBroadcasts
        .groupBy { _.txId }
        .mapValues { broadcasts =>
          val allTxChannels = broadcasts.flatMap { _.channels }
          allTxChannels.toSet
        }

      Task
        .parSequenceUnordered {
          sentBatch
            .map { broadcastTask =>
              val successBroadcasts = txIdToBroadcastChannels.getOrElse(broadcastTask.tx.id(), Set.empty)
              val rest              = broadcastTask.requiredTimes - (successBroadcasts diff broadcastTask.exceptChannels).size
              if (rest > 0) {
                import txBroadcasterSettings.retryDelay
                Task(log.trace(s"It is still necessary '$rest' broadcasts for tx '${broadcastTask.tx.id()}', retry after '$retryDelay'")) *>
                  Task
                    .deferFuture(taskQueue.onNext {
                      BroadcastTask(broadcastTask.tx, rest, broadcastTask.exceptChannels ++ successBroadcasts, certStore = broadcastTask.certStore)
                    })
                    .void
                    .delayExecution(retryDelay)
              } else {
                Task.unit
              }
            }
        }
        .start
        .void
    }

  override def txDiffer(tx: Transaction, certChain: Option[CertChain] = None): Either[ValidationError, Diff] = {
    Either.cond[ValidationError, Unit](isUnknown(tx), (), AlreadyInProcessing(tx.id())) >>
      utx.txDiffer(tx, certChain).leftMap { err =>
        val fullTxBytesHash = ByteStr(crypto.fastHash(tx.bytes()))
        knownTransactions.invalidate(fullTxBytesHash)
        err
      }
  }

  override def broadcast(txWithSize: TransactionWithSize, diff: Diff, certChain: Option[CertChain]): EitherT[Task, CryptoError, Unit] =
    prepareCertStore(certChain)
      .semiflatMap { certStore =>
        val isAdded = utx.forcePut(txWithSize, diff, certChain)

        if (isAdded) {
          Task.fromFuture(taskQueue.onNext(BroadcastTask(txWithSize.tx, txBroadcasterSettings.minBroadcastCount, certStore = certStore))).void
        } else {
          Task.unit
        }
      }

  override def forceBroadcast(tx: Transaction, certChain: Option[CertChain]): EitherT[Task, CryptoError, Unit] =
    prepareCertStore(certChain)
      .semiflatMap { certStore =>
        Task.fromFuture(taskQueue.onNext(BroadcastTask(tx, txBroadcasterSettings.minBroadcastCount, certStore = certStore))).void
      }

  private def prepareCertStore(maybeCertChain: Option[CertChain]): EitherT[Task, CryptoError, CertChainStore] =
    EitherT {
      Task {
        maybeCertChain
          .map(chain => CertChainStore.fromCertificates(chain.toSet.toSeq))
          .getOrElse(Right(CertChainStore.empty))
      }
    }

  override def broadcastIfNew(tx: Transaction, certChain: Option[CertChain]): EitherT[Task, ValidationError, Transaction] = {
    EitherT.fromEither[Task](Either.cond[ValidationError, Unit](isUnknown(tx), (), AlreadyInProcessing(tx.id()))) >>
      prepareCertStore(certChain)
        .leftMap(ValidationError.fromCryptoError)
        .flatMap { certStore =>
          EitherT(Task(utx.putIfNew(tx, certChain)))
            .semiflatMap {
              case (added, _) =>
                if (added)
                  Task.fromFuture(taskQueue.onNext(BroadcastTask(tx, txBroadcasterSettings.minBroadcastCount, certStore = certStore))).as(tx)
                else {
                  Task.pure(tx)
                }
            }
            .leftMap { err =>
              val fullTxBytesHash = ByteStr(crypto.fastHash(tx.bytes()))
              knownTransactions.invalidate(fullTxBytesHash)
              err
            }
        }

  }

  override def broadcastBatchIfNew(txs: Seq[TxFromChannel]): Task[Unit] = Task.defer {
    val unknownTxs = txs.filter {
      case TxFromChannel(_, txWithSize) => isUnknown(txWithSize.tx)
    }

    if (unknownTxs.nonEmpty) {
      val txsByChannels = unknownTxs.groupBy { case TxFromChannel(channel, _) => channel }.toList

      Task.parSequenceUnordered {
        txsByChannels
          .map {
            case (channel, txsByChannel) =>
              processChannelBatch(channel, txsByChannel)
          }
      }.void
    } else {
      Task.unit
    }
  }

  private def processChannelBatch(sender: Channel, txs: Seq[TxFromChannel]): Task[Unit] =
    Task.parSequenceUnordered {
      txs.map {
        case TxFromChannel(_, msg: TxWithSize) =>
          val transactionWithSize = TransactionWithSize(msg.size, msg.tx)
          val tx                  = transactionWithSize.tx
          val maybeCertsInfo      = prepareCertsInfo(msg)

          maybeCertsInfo match {
            case Left(err) =>
              Task(log.debug(s"Unable to put tx '${tx.id()}' to UtxPool: '$err'"))
            case Right(maybeCertsInfo) =>
              val maybeCertChain = maybeCertsInfo.map(_.certChain)
              val certStore      = maybeCertsInfo.fold(CertChainStore.empty)(_.certStore)

              utx.putIfNewWithSize(transactionWithSize, maybeCertChain) match {
                case Right((true, _)) =>
                  Task.fromFuture {
                    taskQueue.onNext(BroadcastTask(tx, txBroadcasterSettings.minBroadcastCount - 1, Set(sender), certStore = certStore))
                  }.void
                case Right((false, _)) =>
                  Task(log.trace(s"Tx '${tx.id()}' wasn't new"))
                case Left(UnsupportedContractApiVersion(contractId, err)) =>
                  Task.fromFuture {
                    log.warn(s"Tx '${tx.id()}' with contractId '$contractId' cannot be inserted into UTX: $err. Rebroadcasting to peers...")
                    taskQueue.onNext(BroadcastTask(tx, txBroadcasterSettings.minBroadcastCount - 1, Set(sender), certStore = certStore))
                  }.void
                case Left(TransactionValidationError(AlreadyInTheState(txId, txHeight), _)) =>
                  Task(log.trace(s"Tx '$txId' already in the state at height '$txHeight'"))
                case Left(error) =>
                  Task(log.debug(s"Unable to put tx '${tx.id()}' to UtxPool: '$error'"))
              }
          }
      }
    }.void

  private def prepareCertsInfo(txWithSize: TxWithSize): Either[TransactionValidationError, Option[CertsInfo]] =
    txWithSize match {
      case BroadcastedTransaction(txInfo, certChainStore) =>
        certChainStore.getCertChains
          .map { chains =>
            // Tx includes one chain, so we can take only the head.
            chains.headOption.map(chain => CertsInfo(certChainStore, chain))
          }
          .leftMap(err => TransactionValidationError(ValidationError.fromCryptoError(err), txInfo.tx))
      case _ =>
        Right(None)
    }

  private[this] val dummy = new Object()

  @inline
  private def isUnknown(tx: Transaction): Boolean = {
    var isUnknown       = false
    val fullTxBytesHash = ByteStr(crypto.fastHash(tx.bytes()))
    knownTransactions.get(fullTxBytesHash,
                          { () =>
                            isUnknown = true
                            dummy
                          })
    isUnknown
  }

  override def close(): Unit = {
    taskQueue.onComplete()
    taskProcessing.cancel()
    currentMinersUpdater.cancel()
  }
}

object EnabledTxBroadcaster {
  private val InitCurrentAndNextRoundMiners: CurrentAndNextMiners = CurrentAndNextMiners(-1, Seq())

  private case class CurrentAndNextMiners(height: Int, miners: Seq[Address])
  private case class BroadcastTask(tx: Transaction, requiredTimes: Int, exceptChannels: Set[Channel] = Set.empty, certStore: CertChainStore)
  private case class SuccessBroadcast(txId: ByteStr, channels: Set[Channel])
  private case class TxWriteResult(txId: ByteStr, writeResults: WriteResult)
  private case class TxAwaitedResult(txId: ByteStr, successChannels: Set[Channel])
  private case class CertsInfo(certStore: CertChainStore, certChain: CertChain)
}
