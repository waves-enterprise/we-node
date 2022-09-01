package com.wavesenterprise.api.http.service

import java.net.{InetAddress, InetSocketAddress, SocketAddress}

import cats.syntax.either._
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.Jsonify.peerInfoJsonify
import com.wavesenterprise.api.http._
import com.wavesenterprise.network.peers.{ActivePeerConnections, PeerDatabase}
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.utils.{ScorexLogging, Time}
import play.api.libs.json._

class PeersApiService(blockchain: Blockchain,
                      peerDatabase: PeerDatabase,
                      connectToPeerFunc: InetSocketAddress => Unit,
                      establishedConnections: ActivePeerConnections,
                      time: Time)
    extends ScorexLogging {
  import PeersApiService._

  // =====================   /peers/allowedNodes   =====================

  def getAllowedNodes: AllowNodesResponse = {
    val participants = blockchain.networkParticipants().map(a => a -> blockchain.participantPubKey(a)).collect {
      case (a, pubKeyOpt) if pubKeyOpt.nonEmpty => AddressWithPublicKey(a.stringRepr, pubKeyOpt.get.publicKeyBase58)
    }
    AllowNodesResponse(participants, time.correctedTime())
  }

  // =====================   /peers/suspended   =====================

  def suspendedPeers: JsArray = {
    JsArray(
      peerDatabase.detailedSuspended
        .take(MaxPeersInResponse)
        .map {
          case (h, t) =>
            Json.obj("hostname" -> h.toString, "timestamp" -> t)
        }
        .toList
    )
  }

  // =====================   /peers/connect   =====================

  def connectToPeer(request: ConnectReq): JsObject = {
    val add: InetSocketAddress = new InetSocketAddress(InetAddress.getByName(request.host), request.port)
    connectToPeerFunc(add)

    Json.obj("hostname" -> add.getHostName, "status" -> "Trying to connect")
  }

  // =====================   /peers/connected   =====================

  def connectedPeerList: JsObject = {
    val peers = establishedConnections.peerInfoList.map(peerInfoJsonify.json)

    Json.obj("peers" -> JsArray(peers))
  }

  // =====================   /peers/all   =====================

  def getAllPeers: JsObject = {
    Json.obj(
      "peers" ->
        JsArray(
          peerDatabase.knownPeers
            .take(MaxPeersInResponse)
            .map {
              case (address, timestamp) =>
                Json.obj(
                  "address"  -> address.toString,
                  "lastSeen" -> timestamp
                )
            }
            .toList))
  }

  // =====================   /peers/hostname/{address}   =====================

  def addressNetworkInfo(addressStr: String): Either[ApiError, NetworkInfo] = {
    Address.fromString(addressStr).leftMap(ApiError.fromCryptoError).map { address =>
      establishedConnections.channelForAddress(address) match {
        case Some(channel) => networkInfoBySocketAddress(channel.remoteAddress())
        case None          => NetworkInfo(connected = false, None, None)
      }
    }
  }

  private def networkInfoBySocketAddress(socketAddress: SocketAddress): NetworkInfo = {
    socketAddress match {
      case isa: InetSocketAddress =>
        NetworkInfo(connected = true, Some(isa.getHostName), Some(isa.getAddress.getHostAddress))
      case _ => NetworkInfo(connected = true, Some("unknown"), Some("unknown"))
    }
  }

}

object PeersApiService {
  val MaxPeersInResponse = 1000

  case class NetworkInfo(connected: Boolean, hostname: Option[String], ip: Option[String])

  implicit val networkInfoFormat: OFormat[NetworkInfo] = Json.format
}
