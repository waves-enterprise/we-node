package com.wavesenterprise.network.snapshot

import cats.data.EitherT
import com.google.common.cache.CacheBuilder
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.api.http.service.PeersIdentityService
import com.wavesenterprise.database.snapshot.{PackedSnapshot, PackedSnapshotInfo, SnapshotWriter}
import com.wavesenterprise.network.{
  ChannelObservable,
  GenesisSnapshotError,
  GenesisSnapshotRequest,
  SnapshotNotification,
  SnapshotRequest,
  closeChannel,
  id
}
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.{Channel, ChannelFutureListener}
import monix.eval.Task
import monix.execution.Scheduler
import scorex.crypto.signatures.Signature

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

class GenesisSnapshotSource(initialRequests: ChannelObservable[GenesisSnapshotRequest],
                            snapshotRequests: ChannelObservable[SnapshotRequest],
                            peersIdentityService: PeersIdentityService,
                            blockchain: Blockchain,
                            snapshotDirectory: String,
                            nodeOwner: PublicKeyAccount)(implicit val scheduler: Scheduler)
    extends SnapshotWriter
    with ScorexLogging
    with AutoCloseable {

  private[this] val dummy = new Object()

  private val identifiedChannels = CacheBuilder
    .newBuilder()
    .expireAfterWrite(1, TimeUnit.MINUTES)
    .maximumSize(10)
    .build[Channel, Object]()

  protected[snapshot] val packedSnapshotTask: Task[PackedSnapshotInfo] = {
    val dataPath = Paths.get(snapshotDirectory)
    val snapshot = dataPath.resolve(PackedSnapshot.PackedSnapshotFile)
    if (Files.exists(snapshot)) {
      Task(snapshot.toFile.length()).map(PackedSnapshotInfo(snapshot, _)).memoizeOnSuccess
    } else {
      Task.raiseError(new SnapshotNotFoundException(s"Snapshot is not found for address '${nodeOwner.toAddress}'")).memoize
    }
  }

  private[this] val initialRequestsProcessing = initialRequests
    .observeOn(scheduler)
    .mapEval {
      case (channel, request) => processInitialRequest(channel, request)
    }
    .executeOn(scheduler)
    .logErr
    .onErrorRestartUnlimited
    .subscribe()

  private[this] val snapshotRequestsProcessing = snapshotRequests
    .observeOn(scheduler)
    .mapEval {
      case (channel, request) => processSnapshotRequest(channel, request)
    }
    .executeOn(scheduler)
    .logErr
    .onErrorRestartUnlimited
    .subscribe()

  private def processInitialRequest(channel: Channel, request: GenesisSnapshotRequest): Task[Unit] = {
    val GenesisSnapshotRequest(sender, genesis, _, signature) = request
    (for {
      packedSnapshot <- EitherT.right[ValidationError](packedSnapshotTask)
      _              <- EitherT.fromEither[Task](peersIdentityService.getIdentity(sender.toAddress, request.bytesWOSig, Signature(signature.arr)))
      _ <- EitherT.fromEither[Task](
        blockchain.heightOf(genesis).toRight[ValidationError](GenericError(s"Genesis block with signature '$genesis' is not found")))
    } yield packedSnapshot).value
      .map {
        case Left(error) =>
          channel.writeAndFlush(GenesisSnapshotError(ApiError.fromValidationError(error))).addListener(ChannelFutureListener.CLOSE)
        case Right(PackedSnapshotInfo(_, size)) =>
          identifiedChannels.put(channel, dummy)
          channel.writeAndFlush(SnapshotNotification(nodeOwner, size))
      }
      .onErrorRecoverWith {
        case e: SnapshotNotFoundException =>
          Task {
            channel.writeAndFlush(GenesisSnapshotError(e.getMessage)).addListener(ChannelFutureListener.CLOSE)
          }
      }
  }.void

  private def processSnapshotRequest(channel: Channel, request: SnapshotRequest): Task[Unit] = Task.defer {
    Option(identifiedChannels.getIfPresent(channel)).fold(
      Task(closeChannel(channel, s"Unidentified channel '${id(channel)}' for snapshot request from '${request.sender}'"))
    ) { _ =>
      Task(log.debug(s"Processing snapshot request from '${request.sender}'...")) >>
        writeSnapshot(channel, request).map(_ => identifiedChannels.invalidate(channel))
    }
  }

  override def close(): Unit = {
    initialRequestsProcessing.cancel()
    snapshotRequestsProcessing.cancel()
  }
}

class SnapshotNotFoundException(message: String) extends RuntimeException(message)
