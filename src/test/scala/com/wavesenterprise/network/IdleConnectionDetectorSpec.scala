package com.wavesenterprise.network

import com.wavesenterprise.TransactionGen
import io.netty.channel.embedded.EmbeddedChannel
import org.scalamock.scalatest.MockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{FreeSpec, Matchers}

import scala.concurrent.duration.DurationInt

class IdleConnectionDetectorSpec extends FreeSpec with Matchers with MockFactory with ScalaCheckPropertyChecks with TransactionGen {

  "should not close an active connection until the timeout" in {
    val handler = new IdleConnectionDetector(500.millis)
    val ch      = new EmbeddedChannel(handler)

    ch.writeInbound("foo")
    Thread.sleep(200)
    ch.runPendingTasks()
    ch.isActive shouldBe true
  }

  "should not close a connection when messages are keep going" in {
    val handler = new IdleConnectionDetector(150.millis)
    val ch      = new EmbeddedChannel(handler)

    (1 to 3).foreach { _ =>
      ch.writeInbound("bar")
      Thread.sleep(50)
      ch.runPendingTasks()
    }

    ch.isActive shouldBe true
  }

  "should close a broken connection" in {
    val handler = new IdleConnectionDetector(200.millis)
    val ch      = new EmbeddedChannel(handler)

    ch.writeInbound("bar")
    Thread.sleep(250)
    ch.runPendingTasks()
    ch.isActive shouldBe false
  }

}
