package com.wavesenterprise.network.peers

import java.net.{InetAddress, InetSocketAddress}

import com.wavesenterprise.utils.ScorexLogging

trait PeerDatabase extends AutoCloseable {

  def addCandidates(socketAddresses: Seq[InetSocketAddress]): Seq[InetSocketAddress]

  def touch(socketAddress: InetSocketAddress): Unit

  def knownPeers: Map[InetSocketAddress, Long]

  def randomPeer(excludedPeers: Iterable[InetSocketAddress]): Option[InetSocketAddress]

  def detailedSuspended: Map[InetAddress, Long]

  def suspend(remoteAddress: InetSocketAddress): Unit
}

object PeerDatabase extends ScorexLogging {

  trait NoOp extends PeerDatabase {
    override def addCandidates(socketAddresses: Seq[InetSocketAddress]): Seq[InetSocketAddress] = Seq.empty

    override def touch(socketAddress: InetSocketAddress): Unit = {}

    override def knownPeers: Map[InetSocketAddress, Long] = Map.empty

    override def randomPeer(connectedPeers: Iterable[InetSocketAddress]): Option[InetSocketAddress] = None

    override val detailedSuspended: Map[InetAddress, Long] = Map.empty

    override def suspend(remoteAddress: InetSocketAddress): Unit = {}

    override def close(): Unit = {}
  }

  object NoOp extends NoOp

}
