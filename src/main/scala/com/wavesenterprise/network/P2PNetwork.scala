package com.wavesenterprise.network

import com.wavesenterprise.metrics.{Metrics, MetricsType}
import com.wavesenterprise.network.Attributes.NodeModeAttribute
import com.wavesenterprise.network.peers.{PeerConnection, PeerDatabase}
import com.wavesenterprise.settings.{NetworkSettings, NodeMode}
import com.wavesenterprise.utils.{PrettyPrintIterable, ScorexLogging}
import monix.execution.Scheduler.{global => connectionScheduler}
import monix.execution.cancelables.SerialCancelable
import org.influxdb.dto.Point

import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class P2PNetwork private (networkSettings: NetworkSettings, networkServer: NS, peerDatabase: PeerDatabase) extends ScorexLogging {

  import P2PNetwork._

  private val connectionsByAddress       = new ConcurrentHashMap[InetSocketAddress, PeerConnectionWrapper]()
  private val maxSimultaneousConnections = networkSettings.maxSimultaneousConnections
  private val attemptConnectionDelay     = networkSettings.attemptConnectionDelay

  private val connectionTask = SerialCancelable()

  def start(): Unit = {
    networkSettings.declaredAddress.foreach { _ =>
      Await.result(networkServer.start(), ServerStartupTimeout)
      networkServer.incomingConnections.foreach(handleIncomingConnection)(connectionScheduler)
    }
    connectionTask := connectionScheduler.scheduleWithFixedDelay(AttemptConnectionInitialDelay, attemptConnectionDelay)(attemptPeerConnections())
  }

  private def attemptPeerConnections(): Unit = {
    val connections = connectionsByAddress.asScala
    logCurrentConnections(connections)
    val connectedPeers = connections.filter {
      case (_, wrapper) => !wrapper.isFailedOrClosed
    }
    peerDatabase.randomPeer(connectedPeers.keys).foreach(connect)
  }

  private def getPeerConnection(remoteAddress: InetSocketAddress): Option[PeerConnectionWrapper] = {
    Option(connectionsByAddress.get(remoteAddress)).filter(_.isAlive)
  }

  /**
    * Accepts incoming connection from peer
    */
  private def handleIncomingConnection(peerConnection: PeerConnection): Unit =
    peerConnection.remoteAddress
      .fold {
        log.warn(s"Incoming connection will be closed, because remote address is unknown")
        peerConnection.close()
      } { remoteAddress =>
        log.trace(s"New incoming connection accepted from '$remoteAddress'")
        val incoming = IncomingPeerConnection(peerConnection)
        connectionsByAddress.compute(remoteAddress, reuseOrAcceptIncomingConnection(incoming))
        peerConnection.onClose(() => handlePeerConnectionClosed(remoteAddress))
      }

  private def reuseOrAcceptIncomingConnection(incoming: IncomingPeerConnection)(remoteAddress: InetSocketAddress,
                                                                                connection: PeerConnectionWrapper): PeerConnectionWrapper = {
    checkExistingConnection(remoteAddress, connection).getOrElse(incoming)
  }

  /**
    * Performs an outgoing connection to a peer
    */
  def connect(remoteAddress: InetSocketAddress): Unit = {
    val existingConnection = getPeerConnection(remoteAddress)
    if (existingConnection.isEmpty) {
      val connectionsCount = connectionsByAddress.size()
      if (connectionsCount < maxSimultaneousConnections) {
        connectionsByAddress.compute(remoteAddress, reuseOrCreateOutgoingConnection)
      } else {
        log.warn(s"Reached maximum of simultaneous connections ('$maxSimultaneousConnections')")
      }
    }
  }

  private def reuseOrCreateOutgoingConnection(remoteAddress: InetSocketAddress, connection: PeerConnectionWrapper): PeerConnectionWrapper = {
    checkExistingConnection(remoteAddress, connection)
      .getOrElse {
        val connectionFuture = networkServer.connect(remoteAddress, handleOutgoingConnectionClosed)
        connectionFuture.onComplete(handleOutgoingConnectionAttempt(remoteAddress))(ExecutionContext.global)
        OutgoingPeerConnection(connectionFuture)
      }
  }

  private def checkExistingConnection(remoteAddress: InetSocketAddress, connection: PeerConnectionWrapper): Option[PeerConnectionWrapper] = {
    Option(connection)
      .collect {
        case existing: PeerConnectionWrapper if existing.isAlive =>
          log.trace(s"Already connected to peer '$remoteAddress'")
          existing
        case existing: PeerConnectionWrapper if existing.isPending =>
          log.trace(s"Connection was already initiated to peer '$remoteAddress'")
          existing
      }
  }

  private def handleOutgoingConnectionAttempt(remoteAddress: InetSocketAddress)(result: Try[PeerConnection]): Unit = result match {
    case Success(peerConnection) =>
      log.trace(s"New outgoing connection accepted on '$remoteAddress'")
      peerDatabase.touch(remoteAddress)
      peerConnection.onClose(() => handlePeerConnectionClosed(remoteAddress))
    case Failure(throwable) =>
      log.error(s"Can't connect to peer with address '$remoteAddress'", throwable)
      peerDatabase.suspend(remoteAddress)
      removePeerConnection(remoteAddress)
  }

  private def removePeerConnection(remoteAddress: InetSocketAddress): Unit = {
    connectionsByAddress.compute(
      remoteAddress,
      (_, connection) => {
        /* On returning 'null' connection will be removed */
        Option(connection).filter(existing => !existing.isFailedOrClosed).orNull
      }
    )
  }

  private def handleOutgoingConnectionClosed(remoteAddress: InetSocketAddress): Unit = {
    connectionsByAddress.compute(
      remoteAddress,
      (_, connection) => {
        Option(connection).filter(existing => existing.isAlive && !existing.isIncoming).orNull
      }
    )
  }

  private def handlePeerConnectionClosed(remoteAddress: InetSocketAddress): Unit = {
    removePeerConnection(remoteAddress)
  }

  def shutdown(): Unit = {
    connectionTask.cancel()
    networkServer.shutdown()
  }
}

object P2PNetwork extends ScorexLogging {

  private val ServerStartupTimeout          = 10.seconds
  private val AttemptConnectionInitialDelay = 1.second

  def apply(networkSettings: NetworkSettings, networkServer: NS, peerDatabase: PeerDatabase): P2PNetwork =
    new P2PNetwork(networkSettings, networkServer, peerDatabase)

  private def logCurrentConnections(connections: scala.collection.concurrent.Map[InetSocketAddress, PeerConnectionWrapper]): Unit = {
    val (incomingConnections, outgoingConnections) = connections.partition(_._2.isIncoming)
    val outgoingAddresses                          = outgoingConnections.keys
    val incomingAddresses                          = incomingConnections.keys

    if (log.logger.isTraceEnabled) {
      log.trace {
        val formattedConnections = connections.map {
          case (peerAddress, connectionWrapper) =>
            val direction = if (connectionWrapper.isIncoming) "incoming" else "outgoing"

            val maybeNodeName         = connectionWrapper.peerConnection.map(_.peerInfo.nodeName)
            val maybeNodeOwnerAddress = connectionWrapper.peerConnection.map(_.peerInfo.nodeOwnerAddress.stringRepr)

            val identifiers = Seq(maybeNodeName, Some(peerAddress.toString), maybeNodeOwnerAddress).flatten.mkString("'", "' -> '", "'")

            val state = {
              if (connectionWrapper.isPending)
                "pending"
              else if (connectionWrapper.isAlive)
                "alive"
              else if (connectionWrapper.isFailedOrClosed)
                "failed or closed"
              else
                "unknown"
            }

            val mode = connectionWrapper.peerConnection
              .filter(_.channel.hasAttr(NodeModeAttribute))
              .fold[NodeMode](NodeMode.Default)(_.channel.attr(NodeModeAttribute).get())
              .toString
              .toLowerCase

            s"[$direction] $identifiers, state = '$state', mode = '$mode'"
        }
        s"Current connections: ${formattedConnections.toPrettyString}"
      }
    } else {
      log.debug(s"Outgoing: ${outgoingAddresses.toPrettyString} ++ Incoming: ${incomingAddresses.toPrettyString}")
    }
    log.info(s"Current connection count: '${connections.size}'")

    Metrics.write(
      MetricsType.Common,
      Point
        .measurement("connections")
        .addField("outgoing", outgoingAddresses.toPrettyString)
        .addField("incoming", incomingAddresses.toPrettyString)
        .addField("n", connections.size)
    )
  }
}

sealed trait PeerConnectionWrapper {

  val initiatedAt: Long = System.currentTimeMillis()

  def connectionFuture: Future[PeerConnection]

  def peerConnection: Option[PeerConnection] = {
    connectionFuture.value.flatMap(_.toOption)
  }

  def isFailedOrClosed: Boolean = {
    connectionFuture.value.exists {
      case Success(c) => c.isClosed
      case Failure(_) => true
    }
  }

  def isAlive: Boolean = connectionFuture.isCompleted && !isFailedOrClosed

  def isPending: Boolean = !connectionFuture.isCompleted

  def isIncoming: Boolean
}

case class OutgoingPeerConnection(connectionFuture: Future[PeerConnection]) extends PeerConnectionWrapper {

  override val isIncoming: Boolean = false
}

case class IncomingPeerConnection(incoming: PeerConnection) extends PeerConnectionWrapper {

  override val connectionFuture: Future[PeerConnection] = Future.successful(incoming)

  override val isIncoming: Boolean = true
}
