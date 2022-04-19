package com.wavesenterprise.network

import cats.implicits._
import com.google.common.cache.CacheBuilder
import com.wavesenterprise.crypto
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.peers.MinersFirstWriter.WriteResult
import com.wavesenterprise.settings.SynchronizationSettings.TxBroadcasterSettings
import com.wavesenterprise.settings.{NodeMode, WESettings}
import com.wavesenterprise.state.diffs.TransactionDiffer.TransactionValidationError
import com.wavesenterprise.state.{ByteStr, Diff}
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

sealed trait TxBroadcaster extends AutoCloseable {
  def utx: UtxPool
  def txDiffer(tx: Transaction): Either[ValidationError, Diff]
  def forceBroadcast(txWithSize: TransactionWithSize, diff: Diff): Unit
  def broadcastIfNew(tx: Transaction): Either[ValidationError, Transaction]
  def broadcast(tx: Transaction): Unit
  def broadcastBatchIfNewWithSize(txs: Seq[TxFromChannel]): Unit
  def activePeerConnections: ActivePeerConnections
}

object TxBroadcaster {
  def apply(settings: WESettings, utx: UtxPool, activePeerConnections: ActivePeerConnections, maxSimultaneousConnections: Int)(
      implicit scheduler: Scheduler): TxBroadcaster = {
    settings.network.mode match {
      case NodeMode.Default =>
        new EnabledTxBroadcaster(settings.synchronization.transactionBroadcaster, utx, activePeerConnections, maxSimultaneousConnections)
      case _ => DisabledTxBroadcaster(utx, activePeerConnections)
    }
  }
}

case class DisabledTxBroadcaster(utx: UtxPool, activePeerConnections: ActivePeerConnections) extends TxBroadcaster {
  override def txDiffer(tx: Transaction): Either[ValidationError, Diff] = utx.txDiffer(tx)

  override def forceBroadcast(txWithSize: TransactionWithSize, diff: Diff): Unit = {}

  override def broadcastIfNew(tx: Transaction): Either[ValidationError, Transaction] = Right(tx)

  override def broadcast(tx: Transaction): Unit = {}

  override def broadcastBatchIfNewWithSize(txs: Seq[TxFromChannel]): Unit = {}

  override def close(): Unit = {}
}

class EnabledTxBroadcaster(
    txBroadcasterSettings: TxBroadcasterSettings,
    val utx: UtxPool,
    val activePeerConnections: ActivePeerConnections,
    maxSimultaneousConnections: Int
)(implicit val scheduler: Scheduler)
    extends TxBroadcaster
    with ScorexLogging {

  private val knownTransactions = CacheBuilder
    .newBuilder()
    .maximumSize(txBroadcasterSettings.knownTxCacheSize)
    .expireAfterWrite(txBroadcasterSettings.knownTxCacheTime.toMillis, TimeUnit.MILLISECONDS)
    .build[ByteStr, Object]

  private case class BroadcastTask(tx: Transaction, requiredTimes: Int, exceptChannels: Set[Channel] = Set.empty)

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
    Task.defer {
      val channelMatcher: ChannelMatcher = { channel: Channel =>
        !task.exceptChannels.contains(channel) && !ActivePeerConnections.channelIsWatcher(channel)
      }

      def writeToGroup(message: AnyRef): WriteResult = {
        if (txBroadcasterSettings.maxBroadcastCount < maxSimultaneousConnections)
          activePeerConnections.writeToRandomSubGroupMinersFirst(message, channelMatcher, txBroadcasterSettings.maxBroadcastCount)
        else
          activePeerConnections.writeMsgMinersFirst(message, channelMatcher)
      }

      val WriteResult(minersChannelGroup, notMinersChannelGroup) = writeToGroup(RawBytes.from(task.tx))

      Task {
        for {
          minersChannels    <- taskFromChannelGroupFuture(minersChannelGroup)
          notMinersChannels <- taskFromChannelGroupFuture(notMinersChannelGroup)
          allSuccessChannels = minersChannels ++ notMinersChannels
        } yield {
          Either.cond(
            allSuccessChannels.nonEmpty,
            SuccessBroadcast(task.tx.id(), allSuccessChannels),
            new RuntimeException(s"Failed to broadcast tx '${task.tx.id()}': empty successful set")
          )
        }
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
                    BroadcastTask(broadcastTask.tx, rest, broadcastTask.exceptChannels ++ successBroadcasts)
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

  override def txDiffer(tx: Transaction): Either[ValidationError, Diff] = {
    Either.cond[ValidationError, Unit](isUnknown(tx), (), AlreadyInProcessing(tx.id())) >>
      utx.txDiffer(tx)
  }

  override def forceBroadcast(txWithSize: TransactionWithSize, diff: Diff): Unit = {
    val added = utx.forcePut(txWithSize, diff)
    if (added) taskQueue.onNext(BroadcastTask(txWithSize.tx, txBroadcasterSettings.minBroadcastCount))
  }

  override def broadcastIfNew(tx: Transaction): Either[ValidationError, Transaction] = {
    Either.cond[ValidationError, Unit](isUnknown(tx), (), AlreadyInProcessing(tx.id())) >>
      utx.putIfNew(tx).map {
        case (added, _) =>
          if (added) taskQueue.onNext(BroadcastTask(tx, txBroadcasterSettings.minBroadcastCount))
          tx
      }
  }

  override def broadcast(tx: Transaction): Unit = taskQueue.onNext(BroadcastTask(tx, txBroadcasterSettings.minBroadcastCount))

  override def broadcastBatchIfNewWithSize(txs: Seq[TxFromChannel]): Unit = {
    val unknownTxs = txs.filter {
      case TxFromChannel(_, TransactionWithSize(_, tx)) => isUnknown(tx)
    }

    if (unknownTxs.nonEmpty) {
      unknownTxs
        .groupBy { case TxFromChannel(channel, _) => channel }
        .foreach {
          case (sender, txByChannels) =>
            txByChannels.foreach {
              case TxFromChannel(_, msg @ TransactionWithSize(_, tx)) =>
                utx.putIfNewWithSize(msg) match {
                  case Right((true, _))  => taskQueue.onNext(BroadcastTask(tx, txBroadcasterSettings.minBroadcastCount - 1, Set(sender)))
                  case Right((false, _)) => log.trace(s"Tx '${tx.id()}' wasn't new")
                  case Left(UnsupportedContractApiVersion(contractId, err)) =>
                    log.warn(s"Tx '${tx.id()}' with contractId '$contractId' cannot be inserted into UTX: $err. Rebroadcasting to peers...")
                    taskQueue.onNext(BroadcastTask(tx, txBroadcasterSettings.minBroadcastCount - 1, Set(sender)))
                  case Left(TransactionValidationError(AlreadyInTheState(txId, txHeight), _)) =>
                    log.trace(s"Tx '$txId' already in the state at height '$txHeight'")
                  case Left(error) => log.debug(s"Unable to put tx '${tx.id()}' to UtxPool: '$error'")
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
