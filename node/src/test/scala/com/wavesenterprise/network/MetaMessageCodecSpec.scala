package com.wavesenterprise.network

import java.util.concurrent.TimeUnit
import com.google.common.cache.{Cache, CacheBuilder}
import com.wavesenterprise.network.message.{ChecksumLength, MessageSpec}
import com.wavesenterprise.network.message.MessageSpec.{PeersV2Spec, TransactionSpec}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.Transaction
import com.wavesenterprise.{TransactionGen, crypto}
import io.netty.buffer.Unpooled.wrappedBuffer
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.embedded.EmbeddedChannel
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class MetaMessageCodecSpec extends AnyFreeSpec with Matchers with MockFactory with ScalaCheckPropertyChecks with TransactionGen {

  private def withSize(tx: Transaction): TransactionWithSize = {
    TransactionWithSize(tx.bytes().length, tx)
  }

  private def newCache(): Cache[ByteStr, Object] = CacheBuilder.newBuilder().expireAfterWrite(3, TimeUnit.MINUTES).build[ByteStr, Object]()

  "should handle one message" in forAll(issueGen) { origTx =>
    val codec = new MetaMessageCodec(newCache())

    val buff = Unpooled.buffer
    write(buff, withSize(origTx), TransactionSpec)

    val ch = new EmbeddedChannel(codec)
    ch.writeInbound(buff)

    val decodedBytes = ch.readInbound[RawBytes]()

    decodedBytes.code shouldBe TransactionSpec.messageCode
    decodedBytes.data shouldEqual origTx.bytes()
  }

  "should handle multiple messages" in forAll(Gen.nonEmptyListOf(issueGen)) { origTxs =>
    val codec = new MetaMessageCodec(newCache())

    val buff = Unpooled.buffer
    origTxs.foreach(tx => write(buff, withSize(tx), TransactionSpec))

    val ch = new EmbeddedChannel(codec)
    ch.writeInbound(buff)

    val decoded = (1 to origTxs.size).map { _ =>
      ch.readInbound[RawBytes]()
    }

    val decodedTxs = decoded.map { x =>
      TransactionSpec.deserializeData(x.data).get.tx
    }

    decodedTxs shouldEqual origTxs
  }

  "should reject an already received transaction" in {
    val tx    = issueGen.sample.getOrElse(throw new RuntimeException("Can't generate a sample transaction"))
    val codec = new MetaMessageCodec(newCache())
    val ch    = new EmbeddedChannel(codec)

    val buff1 = Unpooled.buffer
    write(buff1, withSize(tx), TransactionSpec)
    ch.writeInbound(buff1)

    val buff2 = Unpooled.buffer
    write(buff2, withSize(tx), TransactionSpec)
    ch.writeInbound(buff2)

    ch.inboundMessages().size() shouldEqual 1
  }

  "should not reject an already received GetPeers" in {
    val msg   = KnownPeersV2(Seq(PeerHostname("127.0.0.1", 80)))
    val codec = new MetaMessageCodec(newCache())
    val ch    = new EmbeddedChannel(codec)

    val buff1 = Unpooled.buffer
    write(buff1, msg, PeersV2Spec)
    ch.writeInbound(buff1)

    val buff2 = Unpooled.buffer
    write(buff2, msg, PeersV2Spec)
    ch.writeInbound(buff2)

    ch.inboundMessages().size() shouldEqual 2
  }

  private def write[T <: AnyRef](buff: ByteBuf, msg: T, spec: MessageSpec[T]): Unit = {
    val bytes    = spec.serializeData(msg)
    val checkSum = wrappedBuffer(crypto.fastHash(bytes), 0, ChecksumLength)

    buff.writeInt(MetaMessageCodec.Magic)
    buff.writeByte(spec.messageCode)
    buff.writeInt(bytes.length)
    buff.writeBytes(checkSum)
    buff.writeBytes(bytes)
  }

}
