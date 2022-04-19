package com.wavesenterprise.database.snapshot

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.{ChannelObservable, SnapshotNotification, SnapshotRequest}
import com.wavesenterprise.transaction.LastBlockInfo
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.Channel
import monix.catnap.Semaphore
import monix.eval.Task
import monix.execution.{Ack, Scheduler}
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{Observable, OverflowStrategy}

import scala.concurrent.duration._

case class BroadcasterTimeSpans(broadcastInterval: FiniteDuration)

class SnapshotBroadcaster(nodeOwner: PublicKeyAccount,
                          settings: EnabledSnapshot,
                          statusObservable: Observable[SnapshotStatus],
                          connections: ActivePeerConnections,
                          lastBlockInfo: Observable[LastBlockInfo],
                          requests: ChannelObservable[SnapshotRequest],
                          packSnapshot: Task[Path],
                          val timeSpans: BroadcasterTimeSpans)(implicit val scheduler: Scheduler)
    extends SnapshotWriter
    with ScorexLogging
    with AutoCloseable {

  import SnapshotBroadcaster._

  protected[snapshot] val packedSnapshotTask: Task[PackedSnapshotInfo] = {
    (for {
      path <- packSnapshot
      size <- Task(path.toFile.length)
    } yield PackedSnapshotInfo(path, size)).memoizeOnSuccess
  }

  private[this] val inProgress = ConcurrentHashMap.newKeySet[SnapshotRequest]()
  private[this] val completed  = ConcurrentSubject.publish[SnapshotRequest](OverflowStrategy.DropNew(CompletedBufferSize))

  private[this] val broadcastSnapshotEvent = lastBlockInfo
    .combineLatest(statusObservable)
    .find {
      case (blockInfo, status) =>
        isReadyToBroadcastSnapshot(blockInfo, status)
    }
    .observeOn(scheduler)
    .mapEval { _ =>
      packedSnapshotTask.flatMap(broadcastNotifications)
    }
    .executeOn(scheduler)
    .logErr
    .subscribe()

  private[this] val semaphoreTask = Semaphore[Task](RequestsParallelism).memoizeOnSuccess

  private[this] val requestsProcessing = requests
    .observeOn(scheduler)
    .foreach {
      case (channel, request) =>
        withTryAcquire(request, processRequest(channel, request)).executeOn(scheduler).runAsyncLogErr
    }

  private def withTryAcquire(request: SnapshotRequest, task: Task[Unit]): Task[Unit] = {
    for {
      semaphore <- semaphoreTask
      acquired  <- semaphore.tryAcquire
      _ <- if (acquired) task.guarantee(semaphore.release)
      else Task(log.debug(s"Requests maximum exceeded, dropping request from '${request.sender}'"))
    } yield ()
  }

  private def isReadyToBroadcastSnapshot(blockInfo: LastBlockInfo, status: SnapshotStatus): Boolean = {
    blockInfo.height >= settings.snapshotSendingHeight && status == Verified
  }

  private def broadcastNotifications(packedSnapshot: PackedSnapshotInfo): Task[Unit] =
    Task {
      val notification = SnapshotNotification(nodeOwner, packedSnapshot.size)
      val addresses    = connections.peerInfoList.map(_.nodeOwnerAddress)
      log.debug(s"Broadcasting snapshot notification '$notification' to connected peers '$addresses'")
      connections.broadcast(notification)
    } >> broadcastNotifications(packedSnapshot).delayExecution(timeSpans.broadcastInterval)

  protected[snapshot] def completedRequests(): Observable[SnapshotRequest] = completed

  private def processRequest(channel: Channel, request: SnapshotRequest): Task[Unit] = {
    for {
      _     <- Task(log.debug(s"Processing snapshot request from '${request.sender}'..."))
      added <- Task(inProgress.add(request))
      _ <- if (added) {
        writeToChannel(channel, request).guarantee(Task(inProgress.remove(request)).void)
      } else Task(log.debug(s"Snapshot request from '${request.sender}' already in progress"))
    } yield ()
  }

  protected def writeToChannel(channel: Channel, request: SnapshotRequest): Task[Ack] =
    writeSnapshot(channel, request) >> Task.deferFuture(completed.onNext(request))

  override def close(): Unit = {
    broadcastSnapshotEvent.cancel()
    requestsProcessing.cancel()
    completed.onComplete()
  }
}

object SnapshotBroadcaster {

  protected[snapshot] val RequestsParallelism: Int = 2

  private val CompletedBufferSize: Int = 10

  def apply(nodeOwner: PublicKeyAccount,
            settings: EnabledSnapshot,
            statusObservable: Observable[SnapshotStatus],
            connections: ActivePeerConnections,
            lastBlockInfo: Observable[LastBlockInfo],
            requests: ChannelObservable[SnapshotRequest],
            packSnapshot: Task[Path],
            timeSpans: BroadcasterTimeSpans)(implicit scheduler: Scheduler): SnapshotBroadcaster = {
    new SnapshotBroadcaster(nodeOwner, settings, statusObservable, connections, lastBlockInfo, requests, packSnapshot, timeSpans)(scheduler)
  }
}
