package com.wavesenterprise.network.peers

import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.certs.TestCertBuilder
import io.netty.buffer.Unpooled
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class PeerIdentityResponseSpec extends AnyFreeSpec with Matchers with TestCertBuilder {
  "SuccessPeerIdentityResponse" - {
    "serialize and deserialize" in {
      val keyPair      = keypairGenerator.generateKeyPair()
      val certificates = (1 to 5).map(i => generateSelfSignedCert(keyPair, s"DN$i")).toList
      val pubKey       = PublicKeyAccount(keyPair.getPublic.getEncoded).publicKey
      val spr          = SuccessPeerIdentityResponse(pubKey, certificates)
      val sprBytes     = spr.bytes
      val buf          = Unpooled.buffer(sprBytes.length).writeBytes(sprBytes)
      val decoded      = PeerIdentityResponse.decode(buf, decodeCerts = true)

      decoded shouldBe Right(spr)
    }

    "serialize and deserialize with empty certificates collection" in {
      val keyPair  = keypairGenerator.generateKeyPair()
      val pubKey   = PublicKeyAccount(keyPair.getPublic.getEncoded).publicKey
      val spr      = SuccessPeerIdentityResponse(pubKey, List.empty)
      val sprBytes = spr.bytes
      val buf      = Unpooled.buffer(sprBytes.length).writeBytes(sprBytes)
      val decoded  = PeerIdentityResponse.decode(buf, decodeCerts = true)

      decoded shouldBe Right(spr)
    }

    "deserialize without certificates" in {
      val keyPair  = keypairGenerator.generateKeyPair()
      val pubKey   = PublicKeyAccount(keyPair.getPublic.getEncoded).publicKey
      val spr      = SuccessPeerIdentityResponse(pubKey, List.empty)
      val sprBytes = spr.bytes.init // remove last byte to simulate a message without certificates encoding
      val buf      = Unpooled.buffer(sprBytes.length).writeBytes(sprBytes)
      val decoded  = PeerIdentityResponse.decode(buf, decodeCerts = false)

      decoded shouldBe Right(spr)
    }
  }
}
