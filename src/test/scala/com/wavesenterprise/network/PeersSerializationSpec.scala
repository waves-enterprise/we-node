package com.wavesenterprise.network

import java.net.InetSocketAddress

import com.wavesenterprise.network.message.MessageSpec.PeersSpec
import org.scalatest.{FreeSpec, Matchers}

class PeersSerializationSpec extends FreeSpec with Matchers {

  "encode and decode list of addresses handshake" in {
    val inetSocketAddresses = List(
      new InetSocketAddress("127.0.0.1", 55),
      new InetSocketAddress("76.202.155.45", 55),
      new InetSocketAddress("2001:db8:85a3:8d3:1319:8a2e:370:7348", 8567),
      new InetSocketAddress("2001:db8:82a3:8a3:1319:8a2e:370:7348", 8567),
      new InetSocketAddress("66.6.66.6", 999),
      new InetSocketAddress("2001:db8:82a3:8a3:1319:8a1e:370:7348", 8567),
      new InetSocketAddress("66.6.66.7", 999)
    )
    val peers            = KnownPeers(inetSocketAddresses)
    val serializedData   = PeersSpec.serializeData(peers)
    val deserializedData = PeersSpec.deserializeData(serializedData)

    deserializedData.get.peers should contain theSameElementsAs inetSocketAddresses
  }

  "encode and decode empty list" in {
    val inetSocketAddresses: List[InetSocketAddress] = List.empty
    val peers                                        = KnownPeers(inetSocketAddresses)
    val serializedData                               = PeersSpec.serializeData(peers)
    val deserializedData                             = PeersSpec.deserializeData(serializedData)

    deserializedData.get.peers shouldBe empty
  }

  "encode and decode unresolved peers" in {
    val inetSocketAddresses = List(
      InetSocketAddress.createUnresolved("127.0.0.1", 55),
      InetSocketAddress.createUnresolved("2001:db8:85a3:8d3:1319:8a2e:370:7348", 8567)
    )
    val peers            = KnownPeers(inetSocketAddresses)
    val serializedData   = PeersSpec.serializeData(peers)
    val deserializedData = PeersSpec.deserializeData(serializedData)

    deserializedData.get.peers shouldBe empty
  }

  "encode and decode unresolved peers with resolved " in {
    val resolvedAddresses = List(
      new InetSocketAddress("66.6.66.7", 999),
      new InetSocketAddress("1991:db8:87a3:8a3:1319:8a2e:370:7348", 8567)
    )
    val unresolvedAddresses = List(
      InetSocketAddress.createUnresolved("127.0.0.1", 55),
      InetSocketAddress.createUnresolved("2001:db8:85a3:8d3:1319:8a2e:370:7348", 8567)
    )
    val peers            = KnownPeers(unresolvedAddresses ++ resolvedAddresses)
    val serializedData   = PeersSpec.serializeData(peers)
    val deserializedData = PeersSpec.deserializeData(serializedData)

    deserializedData.get.peers should contain theSameElementsAs resolvedAddresses
  }
}
