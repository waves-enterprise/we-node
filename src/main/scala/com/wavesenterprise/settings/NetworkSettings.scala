package com.wavesenterprise.settings

import cats.Show
import cats.implicits.showInterpolator
import com.google.common.base.Charsets
import com.wavesenterprise.network.TrafficLogger
import com.wavesenterprise.settings.NetworkSettings._
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import java.io.File
import java.net.{InetSocketAddress, URI}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Random

case class UPnPSettings(enable: Boolean, gatewayTimeout: FiniteDuration, discoverTimeout: FiniteDuration)

object UPnPSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[UPnPSettings] = deriveReader

  implicit val toPrintable: Show[UPnPSettings] = { x =>
    import x._
    s"""
       |enable: $enable
       |gatewayTimeout: $gatewayTimeout
       |discoverTimeout: $discoverTimeout
     """.stripMargin
  }
}

case class NetworkSettings(
    file: Option[File],
    bindAddress: String,
    port: Int,
    mode: NodeMode,
    declaredAddress: Option[String],
    nodeName: Option[String],
    nonce: Option[Long],
    knownPeers: Seq[String],
    peersDataResidenceTime: FiniteDuration,
    breakIdleConnectionsTimeout: FiniteDuration,
    maxSimultaneousConnections: Int,
    attemptConnectionDelay: FiniteDuration,
    connectionTimeout: FiniteDuration,
    txBufferSize: Int,
    enablePeersExchange: Boolean,
    peersRequestInterval: FiniteDuration,
    handshakeTimeout: FiniteDuration,
    suspensionResidenceTime: FiniteDuration,
    receivedTxsCacheTimeout: FiniteDuration,
    upnp: UPnPSettings,
    trafficLogger: TrafficLogger.Settings
) {
  require(peersDataResidenceTime > Duration.Zero, "peersDataResidenceTime must be > 0")
  require(breakIdleConnectionsTimeout > Duration.Zero, "breakIdleConnectionsTimeout must be > 0")
  require(maxSimultaneousConnections > 0, "maxSimultaneousConnections must be > 0")
  require(attemptConnectionDelay > Duration.Zero, "attemptConnectionDelay must be > 0")
  require(connectionTimeout > Duration.Zero, "connectionTimeout must be > 0")
  require(txBufferSize > 0, "txBufferSize must be > 0")
  require(peersRequestInterval > Duration.Zero, "peersRequestInterval must be > 0")
  require(handshakeTimeout > Duration.Zero, "handshakeTimeout must be > 0")
  require(suspensionResidenceTime > Duration.Zero, "suspensionResidenceTime must be > 0")
  require(receivedTxsCacheTimeout > Duration.Zero, "receivedTxsCacheTimeout must be > 0")

  val finalNonce: Long = nonce.getOrElse(randomNonce)

  val finalNodeName: String = nodeName.getOrElse(s"Node-$finalNonce")

  require(finalNodeName.getBytes(Charsets.UTF_8).length <= MaxNodeNameBytesLength,
          s"Node name should have length less than $MaxNodeNameBytesLength bytes")

  val bindSocketAddress = new InetSocketAddress(bindAddress, port)

  val maybeDeclaredSocketAddress: Option[InetSocketAddress] = declaredAddress.map { address =>
    val uri = new URI(s"my://$address")
    new InetSocketAddress(uri.getHost, uri.getPort)
  }
}

object NetworkSettings extends WEConfigReaders {

  private val MaxNodeNameBytesLength = 127

  implicit val configReader: ConfigReader[NetworkSettings] = deriveReader

  private def randomNonce: Long = {
    val base = 1000

    (Random.nextInt(base) + base) * Random.nextInt(base) + Random.nextInt(base)
  }

  implicit val toPrintable: Show[NetworkSettings] = { x =>
    import x._
    s"""
       |file: $file
       |bindAddress: $bindSocketAddress
       |declaredAddress: $declaredAddress
       |mode: $mode
       |nodeName: $nodeName
       |nonce: $nonce
       |knownPeers: [${knownPeers.mkString(", ")}]
       |peersDataResidenceTime: $peersDataResidenceTime
       |breakIdleConnectionsTimeout: $breakIdleConnectionsTimeout
       |maxSimultaneousConnections: $maxSimultaneousConnections
       |attemptConnectionDelay: $attemptConnectionDelay
       |connectionTimeout: $connectionTimeout
       |enablePeersExchange: $enablePeersExchange
       |peersRequestInterval: $peersRequestInterval
       |handshakeTimeout: $handshakeTimeout
       |suspensionResidenceTime: $suspensionResidenceTime
       |receivedTxsCacheTimeout: $receivedTxsCacheTimeout
       |uPnPSettings:
       |  ${show"$upnp".replace("\n", "\n--")}
       |trafficLogger:
       |  ${show"$trafficLogger".replace("\n", "\n--")}
     """.stripMargin
  }
}
