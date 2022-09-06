package com.wavesenterprise

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.Ordering.Implicits.infixOrderingOps

class NodeVersionSpec extends AnyFreeSpec with Matchers {
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
    "none are supported for node versions below 1.11.0" in {
      NodeVersion(1, 8, 4).features shouldBe 'empty
    }
  }
}
