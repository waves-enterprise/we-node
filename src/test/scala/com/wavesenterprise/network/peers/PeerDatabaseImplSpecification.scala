package com.wavesenterprise.network.peers

import java.net.InetSocketAddress

import com.wavesenterprise.settings.{NetworkSettings, _}
import org.scalatest.{Matchers, path}
import pureconfig.ConfigSource

class PeerDatabaseImplSpecification extends path.FreeSpecLike with Matchers {
  val host1    = "1.1.1.1"
  val host2    = "2.2.2.2"
  val address1 = new InetSocketAddress(host1, 1)
  val address2 = new InetSocketAddress(host2, 2)

  private val config1 = buildSourceBasedOnDefault {
    ConfigSource.string {
      """node.network {
        |  file = null
        |  known-peers = []
        |  peers-data-residence-time: 2s
        |}""".stripMargin
    }
  }.at("node.network")

  private val settings1 = config1.loadOrThrow[NetworkSettings]

  private val config2 = buildSourceBasedOnDefault {
    ConfigSource.string {
      """node.network {
        |  file = null
        |  known-peers = []
        |  peers-data-residence-time: 10s
        |}""".stripMargin
    }
  }.at("node.network")

  private val settings2 = config2.loadOrThrow[NetworkSettings]

  private val config3 = buildSourceBasedOnDefault {
    ConfigSource
      .string {
        s"""node.network {
           |  file = null
           |  known-peers = ["$host1:1"]
           |  peers-data-residence-time: 2s
           |  enable-peers-exchange: no
           |}""".stripMargin
      }
  }.at("node.network")

  private val settings3 = config3.loadOrThrow[NetworkSettings]

  val database  = PeerDatabaseImpl(settings1)
  val database2 = PeerDatabaseImpl(settings2)
  val database3 = PeerDatabaseImpl(settings3)

  "Peer database" - {
    "new peer should not appear in internal buffer but does not appear in database" in {
      database.knownPeers shouldBe empty
      database.addCandidates(Vector(address1))
      database.randomPeer(Vector.empty) should contain(address1)
      database.knownPeers shouldBe empty
    }

    "new peer should move from internal buffer to database" in {
      database.knownPeers shouldBe empty
      database.addCandidates(Vector(address1))
      database.knownPeers shouldBe empty
      database.touch(address1)
      database.knownPeers.keys should contain(address1)
    }

    "peer should became obsolete after time" in {
      database.touch(address1)
      database.knownPeers.keys should contain(address1)
      sleepLong()
      database.knownPeers shouldBe empty
      database.randomPeer(Vector.empty) shouldBe empty
    }

    "touching peer prevent it from obsoleting" in {
      database.addCandidates(Vector(address1))
      database.touch(address1)
      sleepLong()
      database.touch(address1)
      sleepShort()
      database.knownPeers.keys should contain(address1)
    }

    "random peer should return peers from both from database and buffer" in {
      database2.touch(address1)
      database2.addCandidates(Vector(address2))
      val keys = database2.knownPeers.keys
      keys should contain(address1)
      keys should not contain address2

      val set = (1 to 10).flatMap(_ => database2.randomPeer(Vector.empty)).toSet

      set should contain(address1)
      set should contain(address2)
    }

    "filters out excluded candidates" in {
      database.addCandidates(Vector(address1, address1, address2))

      database.randomPeer(Vector(address1)) should contain(address2)
    }

    "filters out wildcard addresses" in {
      val address = new InetSocketAddress("0.0.0.0", 6864)
      database.addCandidates(Vector(address))
      database.randomPeer(Vector(address1, address2)) shouldBe None
    }

    "known-peers should be always in database" in {
      database3.knownPeers.keys should contain(address1)
      sleepLong()
      database3.knownPeers.keys should contain(address1)
      sleepShort()
      database3.knownPeers.keys should contain(address1)
    }
  }

  private def sleepLong() = Thread.sleep(2200)

  private def sleepShort() = Thread.sleep(200)

}
