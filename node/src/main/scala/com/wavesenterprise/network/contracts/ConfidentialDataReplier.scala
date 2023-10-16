package com.wavesenterprise.network.contracts

import cats.implicits._
import com.google.common.primitives.Ints
import com.wavesenterprise.account.Address
import com.wavesenterprise.crypto
import com.wavesenterprise.crypto.PublicKey
import com.wavesenterprise.crypto.internals.{CryptoError, EncryptedForSingle}
import com.wavesenterprise.database.rocksdb.confidential.ConfidentialRocksDBStorage
import com.wavesenterprise.network.Attributes.TlsAttribute
import com.wavesenterprise.network._
import com.wavesenterprise.network.contracts.ConfidentialDataReplierError.{EncryptionError, NoPeerInfo}
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.settings.contract.ConfidentialDataReplierSettings
import com.wavesenterprise.state.contracts.confidential.ConfidentialDataUnit
import com.wavesenterprise.state.{Blockchain, ContractId}
import com.wavesenterprise.utils.{AsyncLRUCache, Base58, ScorexLogging}
import io.netty.channel.Channel
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.cancelables.SerialCancelable
import monix.reactive.OverflowStrategy.BackPressure

import scala.util.control.NonFatal

sealed abstract class ConfidentialDataReplierError(val message: String) extends RuntimeException(message)

object ConfidentialDataReplierError {
  case class NoPeerInfo(ch: Channel) extends ConfidentialDataReplierError(s"No peer info for channel '${id(ch)}'")

  case class EncryptionError(peerPublicKey: PublicKey, cryptoError: CryptoError)
      extends ConfidentialDataReplierError(s"Failed to encrypt data with public key '${Base58.encode(peerPublicKey.getEncoded)}': $cryptoError")

  case class DataNotFound(request: ConfidentialDataRequest)
      extends ConfidentialDataReplierError(s"Data for $request not found")
  case class PeerNotRegistered(peerAddress: Address) extends ConfidentialDataReplierError(s"Peer '$peerAddress' is not registered in the network")

  case class PeerIsNotConfidentialContractParticipant(peerAddress: Address, contractId: ContractId)
      extends ConfidentialDataReplierError(s"Peer '$peerAddress' is not a participant of the confidential contract '$contractId'")

}

/**
  * Confidential data request handler. Retrieves confidential data from storage and generates response for peers.
  */
class ConfidentialDataReplier(
    protected val blockchain: Blockchain,
    settings: ConfidentialDataReplierSettings,
    requests: ChannelObservable[ConfidentialDataRequest],
    peers: ActivePeerConnections,
    protected val storage: ConfidentialRocksDBStorage,
    cache: AsyncLRUCache[ConfidentialDataRequest, ConfidentialDataUnit]
)(implicit scheduler: Scheduler)
    extends ConfidentialDataReplierMetrics with ScorexLogging {

  import ConfidentialDataReplier._
  import ConfidentialDataReplierError._

  private[this] val requestProcessing = SerialCancelable()

  def run(): Unit = {
    log.debug("Run confidential data replier")
    requestProcessing := requests
      .asyncBoundary(BackPressure(settings.parallelism.value * 2))
      .mapParallelUnordered(settings.parallelism.value) {
        case (channel, request) =>
          processRequest(channel, request).onErrorHandle { ex =>
            log.error(s"Error while processing request '$request' from channel: '$channel'", ex)
          }
      }
      .doOnComplete(Task(log.info("Confidential data replier stops")))
      .logErr
      .onErrorRestartUnlimited
      .subscribe()
  }

  def close(): Unit = requestProcessing.cancel()

  private def processRequest(channel: Channel, request: ConfidentialDataRequest): Task[Unit] = Task.defer {

    for {
      peerAddress <- addressByChannel(channel)
      _ = log.debug(s"Got a $request from '${id(channel)}' with address '$peerAddress'")
      _ = requestsCounter.increment()
      _ <- Task.fromEither(validateRequestingPeer(request.contractId, peerAddress))
      _ <- sendResponse(channel, request)
    } yield {
      log.trace(s"Request '$request' has been processed")
    }

  }

  private def sendResponse(channel: Channel, dataRequest: ConfidentialDataRequest): Task[Unit] = {

    (for {
      confidentialData <- loadData(dataRequest)
      confidentialDataBytes = ConfidentialDataUtils.entriesToBytes(confidentialData.entries)
      startedTimer <- Task(responseEncrypting.start())
      response <- {
        if (channel.hasAttr(TlsAttribute)) {
          Task {
            log.trace(s"Send response with TLS, request '$dataRequest'")
            ConfidentialDataResponse.HasDataResponse(
              confidentialData,
              dataRequest.dataType,
              ConfidentialDataUtils.entriesToBytes(confidentialData.entries)
            )
          }
        } else {
          Task.fromEither {
            log.trace(s"Send response without TLS, request '$dataRequest'")
            encryptResponse(peers, channel, confidentialDataBytes)
              .map { encryptedData =>
                ConfidentialDataResponse.HasDataResponse(
                  confidentialData,
                  dataRequest.dataType,
                  encryptedData
                )
              }
          }
        }
      }
      _ <- Task(startedTimer.stop())
      _ <- countResponse(response.dataType)
      _ <- taskFromChannelFuture(channel.writeAndFlush(response))
    } yield {
      val responseMessage = if (log.logger.isTraceEnabled) response.toString else response.getClass.getSimpleName
      log.debug(s"Sent confidential data response '$responseMessage' to '${id(channel)}' for $dataRequest")
    }) recoverWith {
      case NonFatal(err) =>
        log.debug(s"Failed to build response for $dataRequest", err)
        taskFromChannelFuture(channel.writeAndFlush(ConfidentialDataResponse.NoDataResponse(dataRequest)))
    }
  }

  private def loadData(dataRequest: ConfidentialDataRequest): Task[ConfidentialDataUnit] = {

    cache.getOrLoad(dataRequest) {
      for {
        startedTimer <- Task(loadFromDB.start())
        _            <- Task(log.trace(s"Retrieve confidential data for $dataRequest"))
        maybeData = {
          dataRequest.dataType match {
            case ConfidentialDataType.Input =>
              storage.getInput(dataRequest.commitment)
            case ConfidentialDataType.Output =>
              storage.getOutput(dataRequest.commitment)
          }
        }
        _    <- Task(startedTimer.stop())
        data <- Task.fromEither(maybeData.toRight(DataNotFound(dataRequest)))
      } yield data
    }

  }

  private def addressByChannel(channel: Channel): Task[Address] = Task {
    peers
      .addressForChannel(channel)
      .getOrElse(throw new RuntimeException(s"Failed to find the node-owner address for channel '${channel.remoteAddress}'"))
  }

  private def validateRequestingPeer(contractId: ContractId, peerAddress: Address): Either[ConfidentialDataReplierError, Unit] = {
    for {
      _ <- Either.cond(blockchain.participantPubKey(peerAddress).isDefined, (), PeerNotRegistered(peerAddress))
      _ <- Either.cond(
        blockchain.contract(contractId).exists(_.groupParticipants.contains(peerAddress)),
        (),
        PeerIsNotConfidentialContractParticipant(peerAddress, contractId)
      )
    } yield ()
  }
}

object ConfidentialDataReplier {

  def encryptResponse(peers: ActivePeerConnections, channel: Channel, data: Array[Byte]): Either[ConfidentialDataReplierError, Array[Byte]] = {

    def encryptedToBytes(encrypted: EncryptedForSingle) = {
      Array.concat(
        encrypted.wrappedStructure,
        Ints.toByteArray(encrypted.encryptedData.length),
        encrypted.encryptedData
      )
    }

    for {
      peerConnection <- peers.peerConnection(channel).toRight(NoPeerInfo(channel))
      peerPublicKey  = peerConnection.peerInfo.sessionPubKey.publicKey
      bytesToEncrypt = data
      encrypted <- crypto
        .encrypt(bytesToEncrypt, peerConnection.sessionKey.privateKey, peerPublicKey)
        .leftMap(cause => EncryptionError(peerPublicKey, cause))
      encryptedDataBytes = encryptedToBytes(encrypted)
    } yield encryptedDataBytes
  }
}
