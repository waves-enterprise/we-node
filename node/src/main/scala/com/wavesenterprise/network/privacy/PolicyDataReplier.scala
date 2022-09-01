package com.wavesenterprise.network.privacy

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Source, StreamConverters}
import akka.util.ByteString
import cats.data.EitherT
import cats.effect.Resource
import cats.implicits._
import com.google.common.primitives.Ints
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.ApiError
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.PublicKey
import com.wavesenterprise.crypto.internals.CryptoError
import com.wavesenterprise.database.PrivacyState
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.metrics.privacy.PrivacyMeasurementType._
import com.wavesenterprise.metrics.privacy.PrivacyMetrics
import com.wavesenterprise.network.Attributes.TlsAttribute
import com.wavesenterprise.network._
import com.wavesenterprise.network.netty.handler.stream.{ChunkedStream, StreamWriteProgressListener}
import com.wavesenterprise.network.peers.{ActivePeerConnections, PeerConnection}
import com.wavesenterprise.network.privacy.PolicyDataReplierError.{EncryptionError, NoPeerInfo}
import com.wavesenterprise.network.privacy.PolicyDataStreamEncoding.PolicyDataStreamResponse
import com.wavesenterprise.privacy._
import com.wavesenterprise.settings.privacy.PrivacyReplierSettings
import com.wavesenterprise.state.{Blockchain, ByteStr}
import com.wavesenterprise.utils.{Base58, ScorexLogging}
import io.netty.channel.Channel
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.atomic.AtomicInt
import monix.execution.cancelables.SerialCancelable
import monix.reactive.Observable
import monix.reactive.OverflowStrategy.BackPressure

import java.io.{BufferedInputStream, ByteArrayInputStream, InputStream, SequenceInputStream}
import java.nio.ByteBuffer
import scala.util.control.NonFatal

sealed abstract class PolicyDataReplierError(val message: String) extends RuntimeException(message)

object PolicyDataReplierError {
  case class NoPeerInfo(ch: Channel) extends PolicyDataReplierError(s"No peer info for channel '${id(ch)}'")

  case class EncryptionError(peerPublicKey: PublicKey, cryptoError: CryptoError)
      extends PolicyDataReplierError(s"Failed to encrypt data with public key '${Base58.encode(peerPublicKey.getEncoded)}': $cryptoError")

  case class DataNotFound(policyId: ByteStr, dataHash: PolicyDataHash)
      extends PolicyDataReplierError(s"Data for policy '$policyId' with dataHash '$dataHash' not found")

  case class MetaDataNotFound(policyId: ByteStr, dataHash: PolicyDataHash)
      extends PolicyDataReplierError(s"Meta-data for policy '$policyId' with dataHash '$dataHash' not found")

  case class StorageUnavailable(apiError: ApiError) extends PolicyDataReplierError(s"Storage unavailable: ${apiError.message}")

  case class PeerNotRegistered(peerAddress: Address) extends PolicyDataReplierError(s"Peer '$peerAddress' is not registered in the network")

  case class PeerNotInPolicy(peerAddress: Address, policyId: ByteStr)
      extends PolicyDataReplierError(s"Peer '$peerAddress' is not a participant of the policy '$policyId'")

  case class TooManyRequests(activeCount: Int, limit: Int) extends PolicyDataReplierError(s"Too many requests. Active '$activeCount', limit '$limit'")
}

trait PolicyDataReplier extends AutoCloseable {
  def run(): Unit
  def close(): Unit
}

object NoOpPolicyDataReplier extends PolicyDataReplier {
  override def run(): Unit   = {}
  override def close(): Unit = {}
}

/**
  * Policy data request handler. Retrieves policy data from storage and generates response for peers.
  */
class EnablePolicyDataReplier(
    protected val state: Blockchain with PrivacyState,
    settings: PrivacyReplierSettings,
    requests: ChannelObservable[PrivateDataRequest],
    peers: ActivePeerConnections,
    protected val storage: PolicyStorage,
    strictDataCache: PolicyStrictDataCache
)(implicit scheduler: Scheduler, system: ActorSystem)
    extends PolicyDataReplier
    with PolicyItemTypeSelector
    with ScorexLogging {

  import PolicyDataReplier._
  import PolicyDataReplierError._

  private[this] val ChunkSize = settings.streamChunkSize.toBytes.toInt

  private[this] val requestProcessing = SerialCancelable()

  private[this] val activeStreamRepliesCounter = AtomicInt(0)

  override def run(): Unit = {
    log.debug("Run policy data replier")
    requestProcessing := requests
      .asyncBoundary(BackPressure(settings.parallelism.value * 2))
      .mapParallelUnordered(settings.parallelism.value) {
        case (channel, request) =>
          processRequest(channel, request).onErrorHandle { ex =>
            log.error(s"Error while processing request '$request' from channel: '$channel'", ex)
          }
      }
      .doOnComplete(Task(log.info("Policy data replier stops")))
      .logErr
      .onErrorRestartUnlimited
      .subscribe()
  }

  override def close(): Unit = requestProcessing.cancel()

  private def processRequest(channel: Channel, request: PrivateDataRequest): Task[Unit] = Task.defer {
    import request.{dataHash, policyId}

    val start              = System.currentTimeMillis()
    val requestDescription = s"policyId '$policyId', dataHash '$dataHash'"

    def sendAsStrictEntity: Task[Unit] = sendStrictResponse(channel, policyId, dataHash)

    def sendAsStream: Task[Unit] =
      sendStreamResponse(channel, policyId, dataHash)
        .use { chunkedStream =>
          taskFromChannelFuture {
            val future = channel.writeAndFlush(chunkedStream, channel.newProgressivePromise())
            future.addListener(new StreamWriteProgressListener(s"${id(channel)} $requestDescription uploading"))
            future
          }
        }

    def selectTypeAndSendResponse: Task[Unit] =
      if (state.activatedFeatures.get(BlockchainFeature.PrivacyLargeObjectSupport.id).exists(_ <= state.height)) {
        policyItemType(policyId, dataHash).flatMap {
          case PrivacyDataType.Default => sendAsStrictEntity
          case PrivacyDataType.Large   => sendAsStream
        }
      } else {
        sendAsStrictEntity
      }

    for {
      peerAddress <- addressByChannel(channel)
      _ = log.debug(s"Got a request for $requestDescription from '${id(channel)}' with address '$peerAddress'")
      _ <- Task.fromEither(validateRequestingPeer(request.policyId, peerAddress))
      _ <- selectTypeAndSendResponse
    } yield {
      PrivacyMetrics.writeRawTime(
        measurementType = ReplierRequestProcessing,
        policyId = request.policyId.toString,
        dataHash = request.dataHash.toString,
        start = start
      )
    }
  }

  private def sendStrictResponse(channel: Channel, policyId: ByteStr, dataHash: PolicyDataHash): Task[Unit] = {
    (for {
      loadedData <- loadStrictData(policyId, dataHash)
      response <- {
        if (channel.hasAttr(TlsAttribute))
          Task.pure(GotDataResponse(policyId, dataHash, ByteStr(serializeStrictResponse(loadedData.metaData, loadedData.data))))
        else
          Task.fromEither(encryptStrictResponse(peers, channel, policyId, dataHash, loadedData.data, loadedData.metaData))
      }
      _ <- taskFromChannelFuture(channel.writeAndFlush(response))
    } yield {
      val responseMessage = if (log.logger.isTraceEnabled) response.toString else response.getClass.getSimpleName
      log.debug(s"Sent privacy data response '$responseMessage' to '${id(channel)}' for policyId '$policyId' with dataHash '$dataHash'")
    }) recoverWith {
      case NonFatal(err) =>
        log.debug(s"Failed to build strict response for policy '$policyId' data '$dataHash'", err)
        taskFromChannelFuture(channel.writeAndFlush(NoDataResponse(policyId, dataHash)))
    }
  }

  private def loadStrictData(policyId: ByteStr, dataHash: PolicyDataHash): Task[StrictPolicyData] = {
    strictDataCache.getOrLoad(PolicyDataId(policyId, dataHash)) {
      PrivacyMetrics
        .measureTask(ReplierDataLoading, policyId.toString, dataHash.toString) {
          for {
            _ <- Task(log.trace(s"Retrieve strict data for policyId '$policyId' with dataHash '$dataHash'"))
            maybeMetaData <- EitherT(storage.policyItemMeta(policyId.toString, dataHash.stringRepr))
              .valueOrF(err => Task.raiseError(StorageUnavailable(err)))
            metaData <- Task.fromEither(maybeMetaData.toRight(MetaDataNotFound(policyId, dataHash)))
            maybeData <- EitherT(storage.policyItemData(policyId.toString, dataHash.stringRepr))
              .valueOrF(err => Task.raiseError(StorageUnavailable(err)))
            data <- Task.fromEither(maybeData.toRight(DataNotFound(policyId, dataHash)))
          } yield StrictPolicyData(data.arr, metaData)
        }
    }
  }

  private def sendStreamResponse(channel: Channel, policyId: ByteStr, dataHash: PolicyDataHash): Resource[Task, ChunkedStream] =
    Resource.make {
      Task
        .fromEither(peers.peerConnection(channel).toRight(NoPeerInfo(channel)))
        .flatMap { peerConnection =>
          val connectedPeersCount = peers.connectedPeersCount()
          val maxPeersFullness = math
            .rint(settings.streamMaxPeersFullnessPercentage.value.toDouble / 100 * connectedPeersCount)
            .toInt
            .min(connectedPeersCount - 1)
            .max(1)

          def incrementIfNotExceed(old: Int): Int = if (old >= maxPeersFullness) old else old + 1

          val activeStreamRepliesCount = activeStreamRepliesCounter.getAndTransform(incrementIfNotExceed)
          if (activeStreamRepliesCount >= maxPeersFullness) {
            Task.raiseError(TooManyRequests(activeStreamRepliesCount, maxPeersFullness))
          } else {
            (for {
              loadStreamData <- loadStreamData(policyId, dataHash)
              responseStream <- {
                if (channel.hasAttr(TlsAttribute))
                  buildRawResponseStream(loadStreamData)
                else
                  buildEncryptedResponseStream(peerConnection, loadStreamData)
              }
            } yield {
              ChunkedStreamWithData(responseStream, ChunkSize)
            }).onError {
              case NonFatal(_) => Task(activeStreamRepliesCounter.decrement())
            }
          }
        } recover {
        case err: TooManyRequests =>
          log.warn(s"Replier overloaded", err)
          val errorBody = Array(PolicyDataStreamResponse.TooManyRequests.value)
          new ChunkedStream(new SequenceInputStream(new ByteArrayInputStream(errorBody), new ByteArrayInputStream(StreamTerminator)), ChunkSize)
        case NonFatal(err) =>
          log.debug(s"Failed to build stream response for policy '$policyId' data '$dataHash'", err)
          val notFoundBody = Array(PolicyDataStreamResponse.DataNotFound.value)
          new ChunkedStream(new SequenceInputStream(new ByteArrayInputStream(notFoundBody), new ByteArrayInputStream(StreamTerminator)), ChunkSize)
      }
    } { stream =>
      Task {
        if (stream.isInstanceOf[ChunkedStreamWithData]) {
          activeStreamRepliesCounter.decrement()
        }
        stream.close()
      }
    }

  trait ChunkedStreamWithData extends ChunkedStream

  object ChunkedStreamWithData {
    def apply(in: InputStream): ChunkedStream                 = new ChunkedStream(in) with ChunkedStreamWithData
    def apply(in: InputStream, chunkSize: Int): ChunkedStream = new ChunkedStream(in, chunkSize) with ChunkedStreamWithData
  }

  private def buildRawResponseStream(streamData: StreamPolicyData): Task[SequenceInputStream] = {
    Task {
      import streamData.metaData

      log.trace(s"Build raw response stream for policy '${metaData.policyId}' data '${metaData.hash}'")
      val metaDataBytes = metaData.bytes()
      val existencePart = Array(PolicyDataStreamResponse.HasData.value)
      val metaDataSize  = Ints.toByteArray(metaDataBytes.length)
      val headerStream  = new ByteArrayInputStream(Array.concat(existencePart, metaDataSize, metaDataBytes))

      val dateStream = new BufferedInputStream(
        Source
          .fromPublisher((streamData.dataStream :+ StreamTerminator).toReactivePublisher)
          .map(ByteString.fromArray)
          .runWith(StreamConverters.asInputStream(settings.streamTimeout)),
        ChunkSize
      )

      new SequenceInputStream(headerStream, dateStream)
    }
  }

  private def buildEncryptedResponseStream(peerConnection: PeerConnection, streamData: StreamPolicyData): Task[SequenceInputStream] = Task.defer {
    import streamData.metaData

    log.trace(s"Build encrypted response stream for policy '${metaData.policyId}' data '${metaData.hash}'")

    val peerPublicKey = peerConnection.peerInfo.sessionPubKey.publicKey
    for {
      encryptorSetup <- Task.fromEither {
        crypto.context.algorithms
          .buildEncryptor(peerConnection.sessionKey.privateKey, peerPublicKey, ChunkSize)
          .leftMap(cause => EncryptionError(peerPublicKey, cause))
      }
      (keyBytes, encryptor) = encryptorSetup
      metaDataBytes         = metaData.bytes()
      encryptedMetaDataBytes <- Task(encryptor(metaDataBytes))
      headerStream <- Task {
        val existencePart       = Array(PolicyDataStreamResponse.HasData.value)
        val encryptionChunkSize = Ints.toByteArray(ChunkSize)
        val metaDataSize        = Ints.toByteArray(metaDataBytes.length)
        new ByteArrayInputStream(Array.concat(existencePart, encryptionChunkSize, keyBytes, metaDataSize, encryptedMetaDataBytes))
      }
      encryptedDataStream = new BufferedInputStream(
        Source
          .fromPublisher((streamData.dataStream :+ StreamTerminator).toReactivePublisher)
          .map { bytes =>
            val encryptedBytes = if (bytes.isEmpty) encryptor.doFinal() else encryptor(bytes)
            ByteString.fromArray(encryptedBytes)
          }
          .runWith(StreamConverters.asInputStream(settings.streamTimeout)),
        ChunkSize
      )
    } yield {
      new SequenceInputStream(headerStream, encryptedDataStream)
    }
  }

  private def loadStreamData(policyId: ByteStr, dataHash: PolicyDataHash): Task[StreamPolicyData] = {
    for {
      _ <- Task(log.trace(s"Retrieve data stream for policyId '$policyId' with dataHash '$dataHash'"))
      maybeMetaData <- EitherT(storage.policyItemMeta(policyId.toString, dataHash.stringRepr))
        .valueOrF(err => Task.raiseError(StorageUnavailable(err)))
      metaData <- Task.fromEither(maybeMetaData.toRight(MetaDataNotFound(policyId, dataHash)))
      maybeDataStream <- EitherT(storage.policyItemDataStream(policyId.toString, dataHash.stringRepr))
        .valueOrF(err => Task.raiseError(StorageUnavailable(err)))
      dataStream <- Task.fromEither(maybeDataStream.toRight(DataNotFound(policyId, dataHash)))
    } yield {
      StreamPolicyData(dataStream, metaData)
    }
  }

  private def addressByChannel(channel: Channel): Task[Address] = Task {
    peers
      .addressForChannel(channel)
      .getOrElse(throw new RuntimeException(s"Failed to find the node-owner address for channel '${channel.remoteAddress}'"))
  }

  private def validateRequestingPeer(policyId: ByteStr, peerAddress: Address): Either[PolicyDataReplierError, Unit] = {
    for {
      _ <- Either.cond(state.participantPubKey(peerAddress).isDefined, (), PeerNotRegistered(peerAddress))
      _ <- Either.cond(state.policyRecipients(policyId).contains(peerAddress), (), PeerNotInPolicy(peerAddress, policyId))
    } yield ()
  }
}

object PolicyDataReplier {

  val StreamTerminator: Array[Byte] = Array.empty

  case class StreamPolicyData(dataStream: Observable[Array[Byte]], metaData: PolicyMetaData)
  case class StrictPolicyData(data: Array[Byte], metaData: PolicyMetaData)

  def encryptStrictResponse(peers: ActivePeerConnections,
                            channel: Channel,
                            policyId: ByteStr,
                            dataHash: PolicyDataHash,
                            data: Array[Byte],
                            metaData: PolicyMetaData): Either[PolicyDataReplierError, GotEncryptedDataResponse] = {
    PrivacyMetrics.measureEither(ReplierDataEncrypting, policyId.toString, dataHash.toString) {
      for {
        peerConnection <- peers.peerConnection(channel).toRight(NoPeerInfo(channel))
        peerPublicKey  = peerConnection.peerInfo.sessionPubKey.publicKey
        bytesToEncrypt = serializeStrictResponse(metaData, data)
        encryptedData <- crypto
          .encrypt(bytesToEncrypt, peerConnection.sessionKey.privateKey, peerPublicKey)
          .leftMap(cause => EncryptionError(peerPublicKey, cause))
      } yield GotEncryptedDataResponse(policyId, dataHash, encryptedData)
    }
  }

  def serializeStrictResponse(metaData: PolicyMetaData, data: Array[Byte]): Array[Byte] = {
    val metaBytes = metaData.bytes()
    ByteBuffer
      .allocate(data.length + metaBytes.length + Ints.BYTES)
      .putInt(data.length)
      .put(data)
      .put(metaBytes)
      .array()
  }
}
