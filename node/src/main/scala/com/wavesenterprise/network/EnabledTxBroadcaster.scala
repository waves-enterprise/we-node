package com.wavesenterprise.network

import cats.data.EitherT
import cats.implicits._
import com.google.common.cache.CacheBuilder
import com.wavesenterprise.account.Address
import com.wavesenterprise.consensus.Consensus
import com.wavesenterprise.crypto
import com.wavesenterprise.network.EnabledTxBroadcaster.CurrentAndNextMiners
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.peers.MinersFirstWriter.WriteResultV2
import com.wavesenterprise.certs.{CertChain, CertChainStore}
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
import monix.reactive.OverflowStrategy
import monix.reactive.subjects.ConcurrentSubject

import java.util.concurrent.TimeUnit
import scala.util.control.NoStackTrace

sealed trait TxBroadcaster extends AutoCloseable {
  def utx: UtxPool
  def txDiffer(tx: Transaction, certChain: Option[CertChain] = None): Either[ValidationError, Diff]
  def forceBroadcast(txWithSize: TransactionWithSize, diff: Diff, certChain: Option[CertChain]): Unit
  def broadcastIfNew(tx: Transaction, certChain: Option[CertChain] = None): Either[ValidationError, Transaction]
  def broadcast(tx: Transaction, certChain: Option[CertChain]): Unit
  def broadcastBatchIfNewWithSize(txs: Seq[TxFromChannel]): Unit
  def activePeerConnections: ActivePeerConnections
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
      case _ => DisabledTxBroadcaster(utx, activePeerConnections)
    }
  }
}

case class DisabledTxBroadcaster(utx: UtxPool, activePeerConnections: ActivePeerConnections) extends TxBroadcaster {
  override def txDiffer(tx: Transaction, certChain: Option[CertChain] = None): Either[ValidationError, Diff] = utx.txDiffer(tx, certChain)

  override def forceBroadcast(txWithSize: TransactionWithSize, diff: Diff, certChain: Option[CertChain]): Unit = {}

  override def broadcastIfNew(tx: Transaction, certChain: Option[CertChain]): Either[ValidationError, Transaction] = Right(tx)

  override def broadcast(tx: Transaction, certChain: Option[CertChain]): Unit = {}

  override def broadcastBatchIfNewWithSize(txs: Seq[TxFromChannel]): Unit = {}

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

  @volatile private var cachedCurrentAndNextMiners = EnabledTxBroadcaster.initCurrentAndNextRoundMiners

  private val knownTransactions = CacheBuilder
    .newBuilder()
    .maximumSize(txBroadcasterSettings.knownTxCacheSize)
    .expireAfterWrite(txBroadcasterSettings.knownTxCacheTime.toMillis, TimeUnit.MILLISECONDS)
    .build[ByteStr, Object]

  private case class BroadcastTask(tx: Transaction, requiredTimes: Int, exceptChannels: Set[Channel] = Set.empty, certChain: Option[CertChain] = None)

  private val taskQueue = ConcurrentSubject.publish[BroadcastTask](OverflowStrategy.Unbounded)

  private val taskProcessing: Cancelable = taskQueue
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

  private case class SuccessBroadcast(txId: ByteStr, channels: Set[Channel])

  private def runBroadcasts(batch: Seq[BroadcastTask]): Task[List[SuccessBroadcast]] =
    if (activePeerConnections.isEmpty) {
      Task(log.warn(s"Broadcast failed: empty connected peers")).as(List.empty)
    } else {
      (Task.parSequenceUnordered(batch.map(broadcastAttempt)) <* Task(activePeerConnections.flushWrites()))
        .flatMap { writeResults =>
          Task
            .parSequenceUnordered(writeResults)
            .flatMap { broadcastsResult =>
              val (failed, success) = broadcastsResult.separate

              Task(log.debug(s"Broadcast results: success count '${success.size}', failed count '${failed.size}'")) *>
                Task(logFailedBroadcasts(failed)).as(success)
            }
        }
    }

  private def logFailedBroadcasts(failures: Seq[Throwable]): Unit =
    failures.foreach { ex =>
      log.debug("Broadcast error", ex)
    }

  /** The external task writes transaction to the channels. The internal task waits for the all operations to complete */
  private def broadcastAttempt(task: BroadcastTask): Task[Task[Either[RuntimeException, SuccessBroadcast]]] =
    Task {
      val channelMatcher: ChannelMatcher = { channel: Channel =>
        !task.exceptChannels.contains(channel) && !ActivePeerConnections.channelIsWatcher(channel)
      }

      if (!activePeerConnections.isThereAnyConnection(channelMatcher)) {
        Task.pure[Either[RuntimeException, SuccessBroadcast]](Left(new RuntimeException("No one to broadcast") with NoStackTrace))
      } else {

        def writeToGroup(message: BroadcastedTransaction): WriteResultV2 = {
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
          val currentAndNextMiners = cachedCurrentAndNextMiners.miners
          if (txBroadcasterSettings.maxBroadcastCount < maxSimultaneousConnections) {
            activePeerConnections.writeToRandomSubGroupMinersFirst(message,
                                                                   currentAndNextMiners,
                                                                   channelMatcher,
                                                                   txBroadcasterSettings.maxBroadcastCount)
          } else {
            activePeerConnections.writeMsgMinersFirst(message, currentAndNextMiners, channelMatcher)
          }
        }

        (for {
          certChainStore <- task.certChain match {
            case None => EitherT[Task, RuntimeException, CertChainStore](Task(Right(CertChainStore.empty)))
            case Some(certChain) =>
              EitherT[Task, RuntimeException, CertChainStore](Task {
                CertChainStore.fromCertificates(certChain.toSet.toSeq).leftMap { err =>
                  new RuntimeException(s"Failed to broadcast tx '${task.tx.id()}'. Unable to build CertChainStore from CertChain: ${err.message}")
                  with NoStackTrace
                }
              })
          }
          WriteResultV2(groupFutures) = writeToGroup(BroadcastedTransaction(TransactionWithSize(task.tx.bytes().length, task.tx), certChainStore))
          allSuccessChannels <- {
            EitherT[Task, RuntimeException, Set[Channel]](
              Task
                .sequence(groupFutures.map(taskFromChannelGroupFuture))
                .map(channels => Right(channels.flatten.toSet)))
          }
          successBroadcast <- {
            EitherT[Task, RuntimeException, SuccessBroadcast](Task {
              Either.cond(
                allSuccessChannels.nonEmpty,
                SuccessBroadcast(task.tx.id(), allSuccessChannels),
                new RuntimeException(s"Failed to broadcast tx '${task.tx.id()}': empty successful set") with NoStackTrace
              )
            })
          }
        } yield successBroadcast).value
      }
    }

  private def processBroadcastsResult(sentBatch: Seq[BroadcastTask], successBroadcasts: Seq[SuccessBroadcast]): Task[Unit] = {
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
                  .fromFuture(taskQueue.onNext {
                    BroadcastTask(broadcastTask.tx, rest, broadcastTask.exceptChannels ++ successBroadcasts, broadcastTask.certChain)
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

  override def forceBroadcast(txWithSize: TransactionWithSize, diff: Diff, certChain: Option[CertChain]): Unit = {
    val added = utx.forcePut(txWithSize, diff, certChain)
    if (added) taskQueue.onNext(BroadcastTask(txWithSize.tx, txBroadcasterSettings.minBroadcastCount, certChain = certChain))
  }

  override def broadcastIfNew(tx: Transaction, certChain: Option[CertChain]): Either[ValidationError, Transaction] = {
    Either.cond[ValidationError, Unit](isUnknown(tx), (), AlreadyInProcessing(tx.id())) >>
      utx
        .putIfNew(tx, certChain)
        .map {
          case (added, _) =>
            if (added) taskQueue.onNext(BroadcastTask(tx, txBroadcasterSettings.minBroadcastCount, certChain = certChain))
            tx
        }
        .leftMap { err =>
          val fullTxBytesHash = ByteStr(crypto.fastHash(tx.bytes()))
          knownTransactions.invalidate(fullTxBytesHash)
          err
        }
  }

  override def broadcast(tx: Transaction, certChain: Option[CertChain]): Unit =
    taskQueue.onNext(BroadcastTask(tx, txBroadcasterSettings.minBroadcastCount, certChain = certChain))

  override def broadcastBatchIfNewWithSize(txs: Seq[TxFromChannel]): Unit = {
    val unknownTxs = txs.filter {
      case TxFromChannel(_, txWithSize) => isUnknown(txWithSize.tx)
    }

    if (unknownTxs.nonEmpty) {
      unknownTxs
        .groupBy { case TxFromChannel(channel, _) => channel }
        .foreach {
          case (sender, txByChannels) =>
            txByChannels.foreach {
              case TxFromChannel(_, msg: TxWithSize) =>
                val transactionWithSize = TransactionWithSize(msg.size, msg.tx)
                val tx                  = transactionWithSize.tx
                val maybeCertChain = msg match {
                  case BroadcastedTransaction(_, certChainStore) =>
                    certChainStore.getCertChains
                      .map(_.headOption)
                      .leftMap(err => TransactionValidationError(ValidationError.fromCryptoError(err), tx))
                  case _ => Right(None)
                }

                maybeCertChain match {
                  case Left(err) =>
                    log.debug(s"Unable to put tx '${tx.id()}' to UtxPool: '$err'")
                  case Right(certChain) =>
                    utx.putIfNewWithSize(transactionWithSize, certChain) match {
                      case Right((true, _)) =>
                        taskQueue.onNext(BroadcastTask(tx, txBroadcasterSettings.minBroadcastCount - 1, Set(sender), certChain))
                      case Right((false, _)) =>
                        log.trace(s"Tx '${tx.id()}' wasn't new")
                      case Left(UnsupportedContractApiVersion(contractId, err)) =>
                        log.warn(s"Tx '${tx.id()}' with contractId '$contractId' cannot be inserted into UTX: $err. Rebroadcasting to peers...")
                        taskQueue.onNext(BroadcastTask(tx, txBroadcasterSettings.minBroadcastCount - 1, Set(sender), certChain))
                      case Left(TransactionValidationError(AlreadyInTheState(txId, txHeight), _)) =>
                        log.trace(s"Tx '$txId' already in the state at height '$txHeight'")
                      case Left(error) =>
                        log.debug(s"Unable to put tx '${tx.id()}' to UtxPool: '$error'")
                    }
                }
            }
        }
    }
  }

  private val dummy = new Object()

  @inline
  private def isUnknown(tx: Transaction): Boolean = {
    var isUnknown       = false
    val fullTxBytesHash = ByteStr(crypto.fastHash(tx.bytes()))
    knownTransactions.get(fullTxBytesHash, { () =>
      isUnknown = true
      dummy
    })
    isUnknown
  }

  override def close(): Unit = {
    taskQueue.onComplete()
    taskProcessing.cancel()
  }
}

object EnabledTxBroadcaster {
  private val initCurrentAndNextRoundMiners: CurrentAndNextMiners = CurrentAndNextMiners(-1, Seq())

  private case class CurrentAndNextMiners(height: Int, miners: Seq[Address])

}
