package com.wavesenterprise.network

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.network.message.MessageSpec.{PrivateDataRequestSpec, PrivateDataResponseSpec, RawAttributesSpec, TransactionSpec}
import com.wavesenterprise.transaction.ProvenTransaction
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.embedded.EmbeddedChannel
import org.scalamock.scalatest.MockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.nio.charset.StandardCharsets
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class MessageCodecSpec extends AnyFreeSpec with Matchers with MockFactory with ScalaCheckPropertyChecks with TransactionGen {

  "should block a sender of invalid messages" in {
    val codec = new SpiedMessageCodec
    val ch    = new EmbeddedChannel(codec)

    ch.writeInbound(RawBytes(TransactionSpec.messageCode, "foo".getBytes(StandardCharsets.UTF_8)))
    ch.readInbound[TransactionWithSize]()

    codec.blockCalls shouldBe 1
  }

  "should not block a sender of valid messages" in forAll(randomTransactionGen) { origTx: ProvenTransaction =>
    val codec = new SpiedMessageCodec
    val ch    = new EmbeddedChannel(codec)

    ch.writeInbound(RawBytes.from(origTx))
    val decodedTx = ch.readInbound[TransactionWithSize]()

    decodedTx.tx shouldBe origTx
    codec.blockCalls shouldBe 0
  }

  "serialize PrivateDataRequest" in forAll(privateDataRequestGen) { request =>
    val codec = new SpiedMessageCodec
    val ch    = new EmbeddedChannel(codec)

    ch.writeInbound(RawBytes(PrivateDataRequestSpec.messageCode, PrivateDataRequestSpec.serializeData(request)))
    val read = ch.readInbound[PrivateDataRequest]()
    read shouldBe request
    codec.blockCalls shouldBe 0

    val tl = new TrafficLogger(TrafficLogger.Settings(Set.empty, Set.empty))
    tl.codeOf(request) shouldBe Some(PrivateDataRequestSpec.messageCode)
  }

  "serialize encrypted data response" in forAll(privateEncryptedDataResponseGen) { response =>
    val codec = new SpiedMessageCodec
    val ch    = new EmbeddedChannel(codec)

    ch.writeInbound(RawBytes(PrivateDataResponseSpec.messageCode, PrivateDataResponseSpec.serializeData(response)))
    val read = ch.readInbound[PrivateDataResponse]()
    read shouldBe response
    codec.blockCalls shouldBe 0

    val tl = new TrafficLogger(TrafficLogger.Settings(Set.empty, Set.empty))
    tl.codeOf(response) shouldBe Some(PrivateDataResponseSpec.messageCode)
  }

  "serialize data response" in forAll(privateDataResponseGen) { response =>
    val codec = new SpiedMessageCodec
    val ch    = new EmbeddedChannel(codec)
    ch.attr[Unit](Attributes.TlsAttribute).set(Unit)

    ch.writeInbound(RawBytes(PrivateDataResponseSpec.messageCode, PrivateDataResponseSpec.serializeData(response)))
    val read = ch.readInbound[PrivateDataResponse]()
    read shouldBe response
    codec.blockCalls shouldBe 0

    val tl = new TrafficLogger(TrafficLogger.Settings(Set.empty, Set.empty))
    tl.codeOf(response) shouldBe Some(PrivateDataResponseSpec.messageCode)
  }

  "serialize node attributes" in forAll(nodeAttributesGen, accountGen) {
    case (nodeAttributes, owner) =>
      val codec = new SpiedMessageCodec
      val ch    = new EmbeddedChannel(codec)

      val rawAttributes = RawAttributes.createAndSign(owner, nodeAttributes)
      ch.writeInbound(RawBytes(RawAttributesSpec.messageCode, RawAttributesSpec.serializeData(rawAttributes)))
      val read = ch.readInbound[RawAttributes]()
      read shouldBe rawAttributes
      codec.blockCalls shouldBe 0

      val tl = new TrafficLogger(TrafficLogger.Settings(Set.empty, Set.empty))
      tl.codeOf(rawAttributes) shouldBe Some(RawAttributesSpec.messageCode)
  }

  private class SpiedMessageCodec extends MessageCodec() {
    var blockCalls = 0

    override def block(ctx: ChannelHandlerContext, e: Throwable): Unit = {
      blockCalls += 1
    }
  }

}
