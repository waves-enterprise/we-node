package com.wavesenterprise

import com.wavesenterprise.network.ProtocolFeature
import org.scalatest.{FreeSpec, Matchers}

import scala.Ordering.Implicits.infixOrderingOps

class NodeVersionSpec extends FreeSpec with Matchers {
  "Ordering for NodeVersion" - {
    "equals" in {
      assert(NodeVersion(1, 0, 0) == NodeVersion(1, 0, 0))
      assert(NodeVersion(1, 2, 0) == NodeVersion(1, 2, 0))
      assert(NodeVersion(10, 9, 8) == NodeVersion(10, 9, 8))
    }
    "major versions comparison" in {
      assert(NodeVersion(2, 0, 0) > NodeVersion(1, 0, 0))
      assert(NodeVersion(1, 0, 0) > NodeVersion(0, 0, 0))
      assert(NodeVersion(1, 99, 99) < NodeVersion(2, 0, 0))
    }
    "minor versions comparison" in {
      assert(NodeVersion(1, 1, 0) > NodeVersion(1, 0, 0))
      assert(NodeVersion(1, 99, 0) > NodeVersion(1, 1, 0))
      assert(NodeVersion(1, 2, 0) > NodeVersion(1, 1, 99))
    }
    "patch versions comparison" in {
      assert(NodeVersion(1, 2, 1) > NodeVersion(1, 2, 0))
      assert(NodeVersion(1, 2, 3) > NodeVersion(1, 2, 2))
      assert(NodeVersion(1, 2, 3) < NodeVersion(1, 2, 4))
    }
  }
  "Protocol features" - {
    "none are supported for node versions below 1.2.1" in {
      NodeVersion(1, 2, 0).features shouldBe 'empty
      NodeVersion(1, 0, 0).features shouldBe 'empty
    }
    "and PeersHostnameSupport is supported for versions older than 1.2.1" in {
      NodeVersion(1, 2, 1).features shouldBe Set(ProtocolFeature.PeersHostnameSupport)
      NodeVersion(1, 2, 2).features should contain(ProtocolFeature.PeersHostnameSupport)
    }
  }
}
