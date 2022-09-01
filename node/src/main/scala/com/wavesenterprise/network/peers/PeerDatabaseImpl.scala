package com.wavesenterprise.network.peers

import java.io.File
import java.net.{InetAddress, InetSocketAddress, NetworkInterface}
import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, RemovalNotification}
import com.google.common.collect.EvictingQueue
import com.wavesenterprise.network.inetSocketAddress
import com.wavesenterprise.settings.NetworkSettings
import com.wavesenterprise.utils.{JsonFileStorage, ScorexLogging}
import monix.eval.Coeval

import scala.collection.JavaConverters._
import scala.collection._
import scala.concurrent.duration.FiniteDuration
import scala.util.Random
import scala.util.control.NonFatal

class PeerDatabaseImpl private (settings: NetworkSettings) extends PeerDatabase with ScorexLogging {

  private type PeerRemoved[T]         = RemovalNotification[T, java.lang.Long]
  private type PeerRemovalListener[T] = PeerRemoved[T] => Unit
  private type PeersPersistenceType   = Set[String]

  private val ownAddresses: Set[InetSocketAddress] = {
    val bindAddress = settings.bindSocketAddress
    val isLocal     = Option(bindAddress.getAddress).exists(_.isAnyLocalAddress)
    val localAddresses = if (isLocal) {
      NetworkInterface.getNetworkInterfaces.asScala
        .flatMap(_.getInetAddresses.asScala.map(a => new InetSocketAddress(a, bindAddress.getPort)))
        .toSet
    } else Set(bindAddress)

    localAddresses ++ settings.maybeDeclaredSocketAddress.toSet
  }

  private val defaultNodePort: Int = 6864

  private def cache[T <: AnyRef](timeout: FiniteDuration, removalListener: Option[PeerRemovalListener[T]] = None) = {
    val cacheBuilder = CacheBuilder.newBuilder().expireAfterWrite(timeout.toMillis, TimeUnit.MILLISECONDS)
    (removalListener.map { listener =>
      cacheBuilder.removalListener((removal: PeerRemoved[T]) => listener(removal))
    } getOrElse {
      cacheBuilder
    }).build[T, java.lang.Long]()
  }

  private def nonExpiringKnownPeers(n: PeerRemoved[InetSocketAddress]): Unit =
    if (n.wasEvicted() && knownPeersAddresses.contains(n.getKey))
      peersPersistence.put(n.getKey, n.getValue)

  private val knownPeersAddresses =
    settings.knownPeers.view.map(peerStr => inetSocketAddress(peerStr, defaultNodePort)).filterNot(settings.declaredAddress.contains).toSet
  private val peersPersistence = cache[InetSocketAddress](settings.peersDataResidenceTime, Some(nonExpiringKnownPeers))

  /** Closed outbound connections. Will not be used for new connections. */
  private val suspension      = cache[InetAddress](settings.suspensionResidenceTime)
  private val unverifiedPeers = EvictingQueue.create[InetSocketAddress](100)

  knownPeersAddresses.foreach(touch)

  for (f <- settings.file if f.exists()) try {
    JsonFileStorage.load[PeersPersistenceType](f.getCanonicalPath).foreach { peersPersistence =>
      peersPersistence.foreach(a => touch(inetSocketAddress(a, defaultNodePort)))
    }
    log.info(s"Loaded '${peersPersistence.size}' known peer(s) from ${f.getName}")
  } catch {
    case NonFatal(e) =>
      log.info(s"Couldn't load ${f.getName} file because: '${e.getLocalizedMessage}'. Ignoring, starting all over from known-peers")
  }

  override def addCandidates(socketAddresses: Seq[InetSocketAddress]): Seq[InetSocketAddress] = unverifiedPeers.synchronized {
    socketAddresses.foldLeft(List.empty[InetSocketAddress]) {
      case (added, socketAddress) =>
        val validRemote = isRemoteAndUnknown(socketAddress)
        if (validRemote) {
          unverifiedPeers.add(socketAddress)
          socketAddress +: added
        } else {
          added
        }
    }
  }

  private def isRemoteAndUnknown(socketAddress: InetSocketAddress): Boolean = {
    !socketAddress.getAddress.isAnyLocalAddress &&
    // not a loopback AND not in a bindport
    !(socketAddress.getAddress.isLoopbackAddress && socketAddress.getPort == settings.bindSocketAddress.getPort) &&
    // not presented in database
    Option(peersPersistence.getIfPresent(socketAddress)).isEmpty &&
    // unverified not contains it
    !unverifiedPeers.contains(socketAddress)
  }

  override def touch(socketAddress: InetSocketAddress): Unit = unverifiedPeers.synchronized {
    unverifiedPeers.removeIf(_ == socketAddress)
    peersPersistence.put(socketAddress, System.currentTimeMillis())
  }

  override def knownPeers: immutable.Map[InetSocketAddress, Long] = {
    peersPersistence.cleanUp()
    peersPersistence
      .asMap()
      .asScala
      .map { case (inetSocket, timestamp) => inetSocket -> timestamp.toLong }
      .toMap
  }

  override def detailedSuspended: immutable.Map[InetAddress, Long] = suspension.asMap().asScala.mapValues(_.toLong).toMap

  override def randomPeer(excludedPeers: Iterable[InetSocketAddress]): Option[InetSocketAddress] = unverifiedPeers.synchronized {
    val suspendedHosts: Set[InetAddress] = suspension.asMap().asScala.keySet
    val excluded                         = ownAddresses ++ excludedPeers
    def wasExcludedOrSuspended(isa: InetSocketAddress): Boolean = {
      excluded.contains(isa) || suspendedHosts.contains(isa.getAddress)
    }
    // excluded only contains local addresses, our declared address, and external declared addresses we already have
    // connection to, so it's safe to filter out all matching candidates
    unverifiedPeers.removeIf(excluded.contains)

    val maybeUnverified = Option(unverifiedPeers.peek())
      .filterNot(wasExcludedOrSuspended)
      .map(_ => Coeval(unverifiedPeers.poll()))

    val maybeVerified: Option[Coeval[InetSocketAddress]] = Random
      .shuffle(knownPeers.keySet.filterNot(wasExcludedOrSuspended).toSeq)
      .headOption
      .map(Coeval(_))

    Random
      .shuffle(Seq(maybeUnverified, maybeVerified).flatten)
      .headOption
      .map(_.apply())
  }

  override def close(): Unit = settings.file.foreach { file =>
    persistPeers(file)
  }

  private def persistPeers(file: File): Unit = {
    log.info(s"Saving ${knownPeers.size} known peer(s) to ${file.getName}")
    val rawPeers = for {
      inetAddress <- knownPeers.keySet
      addressHostname = Option(inetAddress.getHostName).filterNot(_.isEmpty)
      address         = Option(inetAddress.getAddress).map(_.getHostAddress)
      addressToSave <- addressHostname.orElse(address)
    } yield s"$addressToSave:${inetAddress.getPort}"

    JsonFileStorage.save[PeersPersistenceType](rawPeers, file.getCanonicalPath)
  }

  override def suspend(remoteAddress: InetSocketAddress): Unit = {
    log.debug(s"Suspending peer with socket address '$remoteAddress'")
    doSuspend(remoteAddress)
  }

  private def doSuspend(socketAddress: InetSocketAddress): Unit = getAddress(socketAddress).foreach { address =>
    unverifiedPeers.synchronized {
      unverifiedPeers.removeIf { x =>
        Option(x.getAddress).contains(address)
      }
      suspension.put(address, System.currentTimeMillis())
    }
  }

  private def getAddress(socketAddress: InetSocketAddress): Option[InetAddress] = {
    val r = Option(socketAddress.getAddress)
    if (r.isEmpty) log.debug(s"Can't obtain an address from $socketAddress")
    r
  }
}

object PeerDatabaseImpl {
  def apply(settings: NetworkSettings): PeerDatabaseImpl = new PeerDatabaseImpl(settings)
}
