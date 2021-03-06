package com.wavesenterprise.network.peers

import com.wavesenterprise.account.Address
import com.wavesenterprise.network.Attributes.NodeModeAttribute
import com.wavesenterprise.network.id
import com.wavesenterprise.network.peers.ActivePeerConnections.channelIsWatcher
import com.wavesenterprise.settings.NodeMode
import com.wavesenterprise.utils.{ReadWriteLocking, ScorexLogging}
import io.netty.channel.group.{ChannelGroupFuture, ChannelMatcher, DefaultChannelGroup}
import io.netty.channel.{Channel, ChannelFuture}
import io.netty.util.concurrent.{EventExecutor, GlobalEventExecutor}

import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

case class PeerSession(channel: Channel, address: Address, peerInfo: PeerInfo)

/**
  * Persists mapping from node owner's address to channel
  */
class ActivePeerConnections extends ReadWriteLocking with ScorexLogging with MinersFirstWriter {

  protected val lock = new ReentrantReadWriteLock()
  //outgoing network channels, added only after successful handshake
  protected val connectedChannels    = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)
  private val establishedConnections = new mutable.HashMap[Address, PeerConnection]
  private val channelToConnection    = new mutable.HashMap[Channel, PeerConnection]
  private val connectionsByPriority  = new mutable.TreeSet[PeerConnection]()(PeerConnection.NonValidatorsFirst)

  // === Mutating methods === //

  def putIfAbsent(peerConnection: PeerConnection): Either[String, PeerConnection] = writeLock {
    val peerNodeOwnerAddress = peerConnection.peerInfo.nodeOwnerAddress
    establishedConnections.get(peerNodeOwnerAddress) match {
      case Some(connection) =>
        log.debug(s"Already connected to peer '$peerNodeOwnerAddress' on channel '${id(connection.channel)}'")
        Left(s"Connection to peer with address '$peerNodeOwnerAddress' already exists")
      case None =>
        putConnection(peerConnection)
        Right(peerConnection)
    }
  }

  private[peers] def replaceNonValidator(peerConnection: PeerConnection): Either[String, PeerConnection] = writeLock {
    connectionsByPriority.headOption.fold(putIfAbsent(peerConnection)) { connection =>
      for {
        _ <- Either.cond(!connection.isValidator, (), "Can't replace non validator peer ??? all peers are validators")
        _ <- putIfAbsent(peerConnection)
        _ = remove(connection)
        _ = log.debug(s"Connection to validator peer '${peerConnection.remoteAddress}' replaced peer '${connection.remoteAddress}'")
      } yield peerConnection
    }
  }

  private def putConnection(peerConnection: PeerConnection): Unit = {
    val peerNodeOwnerAddress = peerConnection.peerInfo.nodeOwnerAddress
    val channel              = peerConnection.channel
    val ctxIdString          = id(channel)

    establishedConnections.put(peerNodeOwnerAddress, peerConnection)
    channelToConnection.put(channel, peerConnection)
    connectionsByPriority.add(peerConnection)
    connectedChannels.add(channel)

    log.trace(s"Added channel '$ctxIdString' with owner address '$peerNodeOwnerAddress' to active peers")
    channel.closeFuture.addListener { _: ChannelFuture =>
      log.trace(s"Channel '$ctxIdString' was closed, remove it from active peers")
      remove(peerConnection)
    }
  }

  def remove(peerConnection: PeerConnection): Unit = writeLock {
    connectedChannels.remove(peerConnection.channel)
    establishedConnections.remove(peerConnection.peerInfo.nodeOwnerAddress)
    channelToConnection.remove(peerConnection.channel)
    connectionsByPriority.remove(peerConnection)
  }

  // === Reading methods === //

  def isEmpty: Boolean = readLock {
    establishedConnections.isEmpty
  }

  def connectedPeersCount(): Int = {
    connectedChannels.size()
  }

  def connectedNonWatchersPeersCount(): Int = readLock {
    establishedConnections.count {
      case (_, connection) => !channelIsWatcher(connection.channel)
    }
  }

  def connections: Set[PeerConnection] = readLock {
    establishedConnections.values.toSet
  }

  def peerInfoList: List[PeerInfo] = readLock {
    establishedConnections.values.map(_.peerInfo).toList
  }

  def channelForAddress(address: Address): Option[Channel] = readLock {
    establishedConnections.get(address).map(_.channel)
  }

  def addressForChannel(channel: Channel): Option[Address] = readLock {
    channelToConnection.get(channel).map(_.peerInfo.nodeOwnerAddress)
  }

  def withAddresses(addressFilter: Address => Boolean, excludeWatchers: Boolean = false): Iterable[PeerSession] = readLock {
    for {
      (address, peerConnection) <- establishedConnections
      if addressFilter(address) && (!excludeWatchers || !channelIsWatcher(peerConnection.channel))
    } yield PeerSession(peerConnection.channel, address, peerConnection.peerInfo)
  }

  def peerConnection(channel: Channel): Option[PeerConnection] = readLock {
    channelToConnection.get(channel)
  }

  // === interface to work with ChannelGroup === //

  def broadcast(message: Any, except: Set[Channel] = Set.empty): ChannelGroupFuture = {
    connectedChannels.writeAndFlush(message, { channel: Channel =>
      !except.contains(channel)
    })
  }

  def broadcastTo(message: Any, participants: Set[Channel]): ChannelGroupFuture = {
    connectedChannels.writeAndFlush(message, { channel: Channel =>
      participants.contains(channel)
    })
  }

  def writeToRandomSubGroup(message: Any,
                            matcher: ChannelMatcher,
                            maxChannelCount: Int,
                            executor: EventExecutor = GlobalEventExecutor.INSTANCE): ChannelGroupFuture = {
    val newGroup = new DefaultChannelGroup(executor)
    val channels = Random
      .shuffle(connectedChannels.iterator().asScala.filter(matcher.matches).toSeq)
      .view
      .take(maxChannelCount)
      .toArray

    channels.foreach(newGroup.add)

    newGroup
      .write(message)
      .addListener { _: ChannelGroupFuture =>
        // After adding to the group, netty adds its own listener for each channel.
        // To clean up resources, we must remove each channel.
        channels.foreach(newGroup.remove)
      }
  }

  def writeMsg(message: AnyRef, matcher: ChannelMatcher): ChannelGroupFuture = {
    connectedChannels.write(message, matcher)
  }

  def flushWrites(): Unit = {
    connectedChannels.flush()
  }

  def close(): ChannelGroupFuture = {
    connectedChannels.close().await()
  }
}

object ActivePeerConnections {
  def channelIsWatcher(channel: Channel): Boolean =
    channel.hasAttr(NodeModeAttribute) && channel.attr(NodeModeAttribute).get() == NodeMode.Watcher
}
