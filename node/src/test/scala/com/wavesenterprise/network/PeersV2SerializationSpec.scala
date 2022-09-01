package com.wavesenterprise.network

import com.wavesenterprise.network.message.MessageSpec.PeersV2Spec
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class PeersV2SerializationSpec extends AnyFreeSpec with Matchers {

  "encode and decode list of hostnames and ports" in {
    val peerHostnames = List(
      PeerHostname("127.0.0.1", 55),
      PeerHostname("76.202.155.45", 55),
      PeerHostname("2001:db8:85a3:8d3:1319:8a2e:370:7348", 8567),
      PeerHostname("2001:db8:82a3:8a3:1319:8a2e:370:7348", 8567),
      PeerHostname("66.6.66.6", 999),
      PeerHostname("2001:db8:82a3:8a3:1319:8a1e:370:7348", 8567),
      PeerHostname("66.6.66.7", 999)
    )
    val peers            = KnownPeersV2(peerHostnames)
    val serializedData   = PeersV2Spec.serializeData(peers)
    val deserializedData = PeersV2Spec.deserializeData(serializedData)

    deserializedData.get.peers should contain theSameElementsAs peerHostnames
  }

  "encode and decode empty list" in {
    val peerHostnames: List[PeerHostname] = List.empty
    val peers                             = KnownPeersV2(peerHostnames)
    val serializedData                    = PeersV2Spec.serializeData(peers)
    val deserializedData                  = PeersV2Spec.deserializeData(serializedData)

    deserializedData.get.peers shouldBe empty
  }

}
