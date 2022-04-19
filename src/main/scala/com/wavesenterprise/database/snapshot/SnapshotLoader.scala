package com.wavesenterprise.database.snapshot

import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.database.snapshot.PackedSnapshot.PackedSnapshotFile
import com.wavesenterprise.network.NetworkServer.MetaMessageCodecHandlerName
import com.wavesenterprise.network.netty.handler.stream.StreamReadProgressListener
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.{ChannelObservable, SnapshotNotification, SnapshotRequest, id}
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.transaction.{BlockchainUpdater, LastBlockInfo, ValidationError}
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.Channel
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{Observable, Observer}

import java.nio.channels.FileChannel
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.concurrent.duration._
import scala.util.Random

case class LoaderTimeSpans(requestTimeout: FiniteDuration, selectionDelay: FiniteDuration, retryLoopDelay: FiniteDuration)

class SnapshotLoader(nodeOwner: PublicKeyAccount,
                     snapshotDirectory: String,
                     val settings: EnabledSnapshot,
                     connections: ActivePeerConnections,
                     lastBlockInfo: Observable[LastBlockInfo],
                     blockchain: Blockchain,
                     notifications: ChannelObservable[SnapshotNotification],
                     val statusObserver: Observer[SnapshotStatus],
                     unpackSnapshot: Task[Either[ValidationError, Unit]],
                     timeSpans: LoaderTimeSpans)(implicit val scheduler: Scheduler)
    extends ScorexLogging
    with SnapshotStatusObserver
    with AutoCloseable {

  import SnapshotLoader._

  protected[snapshot] val snapshotOwners: concurrent.Map[PublicKeyAccount, SnapshotNotification] =
    new ConcurrentHashMap[PublicKeyAccount, SnapshotNotification]().asScala

  private[this] val loadSnapshotEvent = lastBlockInfo
    .find { blockInfo =>
      isReadyToLoadSnapshot(blockInfo)
    }
    .observeOn(scheduler)
    .mapEval { _ =>
      Task(StateSnapshot.exists(snapshotDirectory)).flatMap { exists =>
        if (exists) onStatus(Exists) else loadSnapshotLoop(Set.empty)
      }
    }
    .executeOn(scheduler)
    .logErr
    .onErrorRestartUnlimited
    .subscribe()

  private[this] val notificationsProcessing = notifications
    .filter {
      case (_, notification) =>
        notification.sender != nodeOwner
    }
    .observeOn(scheduler)
    .mapEval {
      case (_, notification) =>
        Task {
          if (snapshotOwners.putIfAbsent(notification.sender, notification).isEmpty) {
            val addresses = connections.peerInfoList.map(_.nodeOwnerAddress)
            log.debug(s"New snapshot notification '$notification', broadcasting to connected peers '$addresses'")
            connections.broadcast(notification)
          }
        }
    }
    .executeOn(scheduler)
    .logErr
    .onErrorRestartUnlimited
    .subscribe()

  private def isReadyToLoadSnapshot(blockInfo: LastBlockInfo): Boolean = {
    blockInfo.height >= settings.snapshotSendingHeight && blockchain.blockHeaderAt(settings.snapshotHeight.value).exists(_.sender != nodeOwner)
  }

  private def selectRandomOwner(exclude: Set[PublicKeyAccount]): Option[PublicKeyAccount] = random(snapshotOwners.keySet -- exclude)

  private def loadSnapshotLoop(exclude: Set[PublicKeyAccount]): Task[Unit] = Task.defer {
    val ownerChannel = selectRandomOwner(exclude).map { owner =>
      owner -> connections.channelForAddress(owner.toAddress)
    }
    ownerChannel match {
      case Some((owner, Some(channel))) if channel.isOpen => downloadSnapshot(owner, channel, exclude)
      case Some((owner, _))                               => loadSnapshotLoop(exclude + owner).delayExecution(timeSpans.selectionDelay)
      case None                                           => loadSnapshotLoop(Set.empty).delayExecution(timeSpans.retryLoopDelay)
    }
  }

  private def downloadSnapshot(owner: PublicKeyAccount, channel: Channel, exclude: Set[PublicKeyAccount]): Task[Unit] = {
    (Task(log.debug(s"${id(channel)} Requesting snapshot from '$owner'")) >>
      executeSnapshotRequest(channel, snapshotOwners(owner)) >>
      Task(log.debug(s"${id(channel)} Snapshot loaded successfully from '$owner'")) >> onDownloadSuccess())
      .onErrorHandleWith { ex =>
        Task(log.error(s"${id(channel)} Failed to load snapshot from '$owner'", ex)) >> loadSnapshotLoop(exclude + owner)
      }
  }

  private def executeSnapshotRequest(channel: Channel, notification: SnapshotNotification): Task[Unit] = Task.defer {
    val snapshotDir  = Paths.get(snapshotDirectory)
    val snapshotFile = snapshotDir.resolve(PackedSnapshotFile)

    Files.createDirectories(snapshotDir)
    if (Files.notExists(snapshotFile)) Files.createFile(snapshotFile)

    Task(FileChannel.open(snapshotFile, StandardOpenOption.APPEND)).bracket { fileChannel =>
      val offset          = fileChannel.size()
      val listener        = new StreamReadProgressListener(s"${id(channel)} snapshot loading", offset, Some(notification.size))
      val snapshotHandler = new SnapshotDataStreamHandler(channel, fileChannel, listener)

      channel.pipeline().addBefore(MetaMessageCodecHandlerName, SnapshotDataStreamHandler.Name, snapshotHandler)
      channel.writeAndFlush(SnapshotRequest(nodeOwner, offset))

      (Task.fromFuture(snapshotHandler.start).timeout(timeSpans.requestTimeout) *>
        Task(log.info("Started snapshot loading")) *>
        Task.fromFuture(snapshotHandler.completion)).guarantee(Task(snapshotHandler.dispose()))
    } { fileChannel =>
      Task(fileChannel.close())
    }
  }

  private def onDownloadSuccess(): Task[Unit] = {
    unpackSnapshot
      .flatMap {
        case Right(_)  => onStatus(Exists)
        case Left(err) => onStatus(Failed(err.toString))
      }
      .onErrorHandleWith { throwable =>
        onStatus(Failed(throwable))
      }
  }

  override def close(): Unit = {
    loadSnapshotEvent.cancel()
    notificationsProcessing.cancel()
  }
}

object SnapshotLoader {

  def apply(nodeOwner: PublicKeyAccount,
            snapshotDirectory: String,
            settings: EnabledSnapshot,
            connections: ActivePeerConnections,
            blockchain: BlockchainUpdater with Blockchain,
            notifications: ChannelObservable[SnapshotNotification],
            statusObserver: Observer[SnapshotStatus],
            unpackSnapshot: Task[Either[ValidationError, Unit]],
            timeSpans: LoaderTimeSpans)(implicit scheduler: Scheduler): SnapshotLoader = {
    new SnapshotLoader(nodeOwner,
                       snapshotDirectory,
                       settings,
                       connections,
                       blockchain.lastBlockInfo,
                       blockchain,
                       notifications,
                       statusObserver,
                       unpackSnapshot,
                       timeSpans)(scheduler)
  }

  private def random[T](s: collection.Set[T]): Option[T] = {
    if (s.isEmpty) None
    else {
      val n = Random.nextInt(s.size)
      Some(s.iterator.drop(n).next())
    }
  }
}
