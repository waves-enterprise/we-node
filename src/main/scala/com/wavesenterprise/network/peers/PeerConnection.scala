package com.wavesenterprise.network.peers

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.network.Attributes.ValidatorAttribute
import com.wavesenterprise.network.{ChannelExt, id}
import com.wavesenterprise.utils.ScorexLogging
import io.netty.channel.Channel

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicBoolean

class PeerConnection(val channel: Channel, val peerInfo: PeerInfo, val sessionKey: PrivateKeyAccount) extends ScorexLogging {

  private[this] val disconnected = new AtomicBoolean(false)

  channel.closeFuture().addListener { _: io.netty.util.concurrent.Future[_] =>
    disconnected.set(true)
  }

  def isValidator: Boolean = channel.hasAttr(ValidatorAttribute)

  val remoteAddress: Option[InetSocketAddress] = {
    channel.remoteAddressEither match {
      case Right(remoteAddress) =>
        Some(remoteAddress)
      case Left(ex) =>
        log.warn(s"Cannot get remote address for '${id(channel)}'", ex)
        None
    }
  }

  def isClosed: Boolean = disconnected.get()

  private[network] def onClose(f: () => Unit): Unit = {
    channel.closeFuture().addListener { _: io.netty.util.concurrent.Future[_] =>
      f()
    }
  }

  private[network] def close(): Unit = {
    if (disconnected.compareAndSet(false, true)) {
      channel.close()
    }
  }
}

object PeerConnection {

  implicit val NonValidatorsFirst: Ordering[PeerConnection] = Ordering.by[PeerConnection, (Boolean, Int)](p => (p.isValidator, p.hashCode()))
}
