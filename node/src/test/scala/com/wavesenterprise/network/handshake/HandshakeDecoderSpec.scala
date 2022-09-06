package com.wavesenterprise.network.handshake

import com.google.common.primitives.{Ints, Longs}
import com.wavesenterprise.{ApplicationInfo, NoShrink, NodeVersion, TransactionGen, crypto}
import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import org.scalacheck.{Arbitrary, Gen}
import org.scalamock.scalatest.MockFactory
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import scorex.crypto.signatures.Signature

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class HandshakeDecoderSpec extends AnyFreeSpec with Matchers with MockFactory with ScalaCheckPropertyChecks with TransactionGen with NoShrink {

  private val applicationInstanceInfo = ApplicationInfo(
    applicationName = "waves-enterpriseI",
    nodeVersion = NodeVersion(1, 2, 3),
    consensusType = "pos",
    nodeName = "test",
    nodeNonce = 4,
    declaredAddress = None
  )

  private val payloadGen: Gen[HandshakeV3Payload] = for {
    chainId         <- Gen.alphaChar
    majorVersion    <- Gen.posNum[Int]
    minorVersion    <- Gen.posNum[Int]
    pathVersion     <- Gen.posNum[Int]
    consensusType   <- Gen.alphaStr.filter(_.nonEmpty)
    declaredAddress <- Gen.option(Gen.const(new InetSocketAddress("0.0.0.0", 123)))
    nodeNonce       <- Gen.posNum[Long]
    nodeName        <- Gen.alphaStr.filter(_.nonEmpty)
    sessionPubKey   <- accountGen
  } yield {
    HandshakeV3Payload(chainId,
                       NodeVersion(majorVersion, minorVersion, pathVersion),
                       consensusType,
                       declaredAddress,
                       nodeNonce,
                       nodeName,
                       sessionPubKey)
  }

  private def createChannel(successReadFunc: SignedHandshake => Unit) = {
    new EmbeddedChannel(
      new HandshakeDecoder(),
      new ChannelInboundHandlerAdapter {
        override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = msg match {
          case x: SignedHandshake => successReadFunc(x)
          case _                  =>
        }
      }
    )
  }

  "handshake v3 payload serialization round-trip" in {
    forAll(payloadGen) { payload =>
      val bytes = payload.bytes()
      HandshakeV3Payload.parse(bytes) shouldBe payload
    }
  }

  "encode and decode signed handshake V2" in {
    forAll(accountGen) { acc =>
      val sessionKey        = crypto.generatePublicKey
      val originalHandshake = SignedHandshakeV2.createAndSign(applicationInstanceInfo, sessionKey, acc)

      val buffer = Unpooled.buffer
      originalHandshake.encode(buffer)

      val decodedHandshake = SignedHandshake.decode(buffer)

      originalHandshake shouldEqual decodedHandshake

      originalHandshake.bytes() should contain theSameElementsAs decodedHandshake.bytes()
    }
  }

  "encode and decode signed handshake V3" in {
    forAll(accountGen, payloadGen) { (acc, payload) =>
      val sessionKey = crypto.generatePublicKey
      val originalHandshake = SignedHandshakeV3.createAndSign(
        chainId = payload.chainId,
        nodeVersion = payload.nodeVersion,
        consensusType = payload.consensusType,
        declaredAddress = payload.declaredAddress,
        nodeNonce = payload.nodeNonce,
        nodeName = payload.nodeName,
        sessionPubKey = sessionKey,
        ownerKey = acc
      )

      val buffer = Unpooled.buffer
      originalHandshake.encode(buffer)

      val decodedHandshake = SignedHandshake.decode(buffer)

      originalHandshake shouldEqual decodedHandshake

      originalHandshake.bytes() should contain theSameElementsAs decodedHandshake.bytes()
    }
  }

  "should read a signed handshake and remove itself from the pipeline" in {
    var mayBeDecodedHandshake: Option[SignedHandshake] = None

    val account       = accountGen.sample.get
    val sessionKey    = crypto.generatePublicKey
    val origHandshake = SignedHandshakeV3.createAndSign(applicationInstanceInfo, sessionKey, account)

    val channel = createChannel((handshake: SignedHandshake) => mayBeDecodedHandshake = Some(handshake))

    val buff = Unpooled.buffer
    origHandshake.encode(buff)
    buff.writeCharSequence("foo", StandardCharsets.UTF_8)

    channel.writeInbound(buff)
    mayBeDecodedHandshake should contain(origHandshake)
  }

  "should read a signed handshake with declaredAddress and remove itself from the pipeline" in {
    var mayBeDecodedHandshake: Option[SignedHandshake] = None

    val inetSocketAddress             = new InetSocketAddress("127.0.0.1", 2892)
    val customApplicationInstanceInfo = applicationInstanceInfo.copy(declaredAddress = Some(inetSocketAddress))
    val account                       = accountGen.sample.get
    val sessionKey                    = crypto.generatePublicKey
    val origHandshake                 = SignedHandshakeV3.createAndSign(customApplicationInstanceInfo, sessionKey, account)

    val channel = createChannel((handshake: SignedHandshake) => mayBeDecodedHandshake = Some(handshake))

    val buff = Unpooled.buffer
    origHandshake.encode(buff)
    buff.writeCharSequence("foo", StandardCharsets.UTF_8)

    channel.writeInbound(buff)
    mayBeDecodedHandshake should contain(origHandshake)
  }

  "should comparing SignedHandshakeV3 in a right way" in {
    val signatureGen = byteArrayGen(crypto.SignatureLength)
    val setupGen = for {
      signature1 <- signatureGen
      signature2 <- signatureGen
      address    <- addressGen
      payload    <- payloadGen
    } yield (Signature(signature1), Signature(signature2), address, payload)

    forAll(setupGen) {
      case (s1, s2, address, payload) =>
        val sh1 = SignedHandshakeV3(payload = payload, payload.bytes(), address, s1)
        val sh2 = SignedHandshakeV3(payload = payload, payload.bytes(), address, s2)
        val sh3 = SignedHandshakeV3(payload = payload, payload.bytes(), address, s1)

        sh1 should not equal sh2
        sh1 shouldEqual sh3
    }
  }

  private val invalidHandshakeBytes: Gen[Array[Byte]] = {
    // To bypass situations where the appNameLength > whole buffer and HandshakeDecoder waits for next bytes
    val appName       = "x" * Byte.MaxValue
    val nodeName      = "y" * Byte.MaxValue
    val consensusType = "z" * Byte.MaxValue

    val appNameBytes       = appName.getBytes(StandardCharsets.UTF_8)
    val versionBytes       = Array(1, 2, 3).flatMap(Ints.toByteArray)
    val consensusTypeBytes = consensusType.getBytes(StandardCharsets.UTF_8)
    val nodeNameBytes      = nodeName.getBytes(StandardCharsets.UTF_8)
    val nonceBytes         = Longs.toByteArray(1)
    val timestampBytes     = Longs.toByteArray(System.currentTimeMillis() / 1000)

    val validDeclaredAddressLen = Set(0, 8, 20)
    val invalidBytesGen = Gen.listOfN(3, Arbitrary.arbByte.arbitrary).filter {
      case List(appNameLen, nodeNameLen, declaredAddressLen) =>
        !(appNameLen == appNameBytes.size || nodeNameLen == nodeNameBytes.size ||
          validDeclaredAddressLen.contains(declaredAddressLen))
      case _ =>
        false
    }

    invalidBytesGen.map {
      case List(appNameLen, nodeNameLen, declaredAddressLen) =>
        Array(appNameLen) ++
          appNameBytes ++
          versionBytes ++
          consensusTypeBytes ++
          Array(nodeNameLen) ++
          nodeNameBytes ++
          nonceBytes ++
          Array(declaredAddressLen) ++
          timestampBytes
    }
  }

  "should close a node connection that sends an invalid handshake" in forAll(invalidHandshakeBytes) { bytes =>
    val decoder = new SpiedHandshakeDecoder
    val channel = new EmbeddedChannel(decoder)

    val buff = Unpooled.buffer
    buff.writeBytes(bytes)

    channel.writeInbound(buff)
    decoder.blockCalls shouldBe >(0)
  }

  private class SpiedHandshakeDecoder extends HandshakeDecoder() {
    var blockCalls = 0

    override protected def close(ctx: ChannelHandlerContext, e: Throwable): Unit = {
      blockCalls += 1
    }
  }

}
