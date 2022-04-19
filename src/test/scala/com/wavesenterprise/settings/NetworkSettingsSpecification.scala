package com.wavesenterprise.settings

import org.scalatest.{FlatSpec, Matchers}
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderException

import java.net.InetSocketAddress
import scala.concurrent.duration._

class NetworkSettingsSpecification extends FlatSpec with Matchers {

  "NetworkSpecification" should "read values from config" in {
    val configSource = buildSourceBasedOnDefault {
      ConfigSource.string(
        """node.network {
        |  bind-address: "127.0.0.1"
        |  port: 6868
        |  mode: "default"
        |  node-name: "default-node-name"
        |  declared-address: "127.0.0.1:6868"
        |  nonce: 0
        |  known-peers = ["8.8.8.8:6868", "4.4.8.8:6868"]
        |  local-only: no
        |  peers-data-residence-time: 1d
        |  black-list-residence-time: 10m
        |  break-idle-connections-timeout: 53s
        |  max-simultaneous-connections = 30
        |  attempt-connection-delay = 5s
        |  connection-timeout: 30s
        |  peers-request-interval: 2m
        |  black-list-threshold: 50
        |  unrequested-packets-threshold: 100
        |  tx-buffer-size: 1000
        |  upnp {
        |    enable: yes
        |    gateway-timeout: 10s
        |    discover-timeout: 10s
        |  }
        |  traffic-logger {
        |    ignore-tx-messages = [28]
        |    ignore-rx-messages = [23]
        |  }
        |}""".stripMargin
      )
    }

    val networkSettings = configSource.at("node.network").loadOrThrow[NetworkSettings]

    networkSettings.bindSocketAddress shouldBe new InetSocketAddress("127.0.0.1", 6868)
    networkSettings.mode shouldBe NodeMode.Default
    networkSettings.finalNodeName shouldBe "default-node-name"
    networkSettings.maybeDeclaredSocketAddress shouldBe Some(new InetSocketAddress("127.0.0.1", 6868))
    networkSettings.finalNonce shouldBe 0
    networkSettings.knownPeers shouldBe List("8.8.8.8:6868", "4.4.8.8:6868")
    networkSettings.peersDataResidenceTime shouldBe 1.day
    networkSettings.breakIdleConnectionsTimeout shouldBe 53.seconds
    networkSettings.maxSimultaneousConnections shouldBe 30
    networkSettings.attemptConnectionDelay shouldBe 5.seconds
    networkSettings.connectionTimeout shouldBe 30.seconds
    networkSettings.peersRequestInterval shouldBe 2.minutes
    networkSettings.upnp.enable shouldBe true
    networkSettings.upnp.gatewayTimeout shouldBe 10.seconds
    networkSettings.upnp.discoverTimeout shouldBe 10.seconds
    networkSettings.trafficLogger.ignoreTxMessages shouldBe Set(28)
    networkSettings.trafficLogger.ignoreRxMessages shouldBe Set(23)
    networkSettings.txBufferSize shouldBe 1000
  }

  it should "generate random nonce" in {
    val configSource    = buildSourceBasedOnDefault(ConfigSource.empty).at("node.network")
    val networkSettings = configSource.loadOrThrow[NetworkSettings]

    networkSettings.finalNonce should not be 0
  }

  it should "build node name using nonce" in {
    val configSource    = buildSourceBasedOnDefault(ConfigSource.string("node.network.nonce = 12345")).at("node.network")
    val networkSettings = configSource.loadOrThrow[NetworkSettings]

    networkSettings.finalNonce shouldBe 12345
    networkSettings.finalNodeName shouldBe "Node-12345"
  }

  it should "build node name using random nonce" in {
    val configSource    = buildSourceBasedOnDefault(ConfigSource.empty).at("node.network")
    val networkSettings = configSource.loadOrThrow[NetworkSettings]

    networkSettings.finalNonce should not be 0
    networkSettings.finalNodeName shouldBe s"Node-${networkSettings.finalNonce}"
  }

  it should "fail with IllegalArgumentException on too long node name" in {
    val configSource = buildSourceBasedOnDefault {
      ConfigSource.string(
        "node.network.node-name = очень-длинное-название-в-многобайтной-кодировке-отличной-от-однобайтной-кодировки-американского-института-стандартов")
    }.at("node.network")

    intercept[IllegalArgumentException] {
      configSource.loadOrThrow[NetworkSettings]
    }
  }

  it should "read config with NodeMode.Watcher" in {
    val configSource    = buildSourceBasedOnDefault(ConfigSource.string("node.network.mode = watcher")).at("node.network")
    val networkSettings = configSource.loadOrThrow[NetworkSettings]

    networkSettings.mode shouldBe NodeMode.Watcher
  }

  it should "fail with IllegalArgumentException on unknown NodeMode" in {
    val configSource = buildSourceBasedOnDefault(ConfigSource.string("node.network.mode = unknown")).at("node.network")

    intercept[ConfigReaderException[NoSuchElementException]] {
      configSource.loadOrThrow[NetworkSettings]
    }
  }
}
