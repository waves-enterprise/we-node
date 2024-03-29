package com.wavesenterprise.transaction.assets.exchange

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.assets.exchange.OrderJson._
import com.wavesenterprise.transaction.smart.Verifier
import com.wavesenterprise.utils.Base58
import com.wavesenterprise.wallet.Wallet
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.Ignore
import play.api.libs.json._
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

@Ignore
class OrderJsonSpecification extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen {

  property("Read Order from json") {
    val pk               = Wallet.generateNewAccount()
    val pubKeyStr        = pk.publicKeyBase58
    val matcherPublicKey = Wallet.generateNewAccount().publicKeyBase58

    val json = Json.parse(s"""
        {
          "senderPublicKey": "$pubKeyStr",
          "matcherPublicKey": "$matcherPublicKey",
          "assetPair": {
            "amountAsset": "29ot86P3HoUZXH1FCoyvff7aeZ3Kt7GqPwBWXncjRF2b",
            "priceAsset": "GEtBMkg419zhDiYRXKwn2uPcabyXKqUqj4w3Gcs1dq44"
          },
          "orderType": "buy",
          "amount": 0,
          "matcherFee": 0,
          "price": 0,
          "timestamp": 0,
          "expiration": 0,
          "signature": "signature"
        } """)

    json.validate[Order] match {
      case JsError(e) =>
        fail("Error: " + e.toString())
      case JsSuccess(o, _) =>
        o.senderPublicKey shouldBe PublicKeyAccount(pk.publicKey)
        o.matcherPublicKey shouldBe PublicKeyAccount(Base58.decode(matcherPublicKey).get)
        o.assetPair.amountAsset.get shouldBe ByteStr.decodeBase58("29ot86P3HoUZXH1FCoyvff7aeZ3Kt7GqPwBWXncjRF2b").get
        o.assetPair.priceAsset.get shouldBe ByteStr.decodeBase58("GEtBMkg419zhDiYRXKwn2uPcabyXKqUqj4w3Gcs1dq44").get
        o.price shouldBe 0
        o.amount shouldBe 0
        o.matcherFee shouldBe 0
        o.timestamp shouldBe 0
        o.expiration shouldBe 0
        o.signature shouldBe Base58.decode("signature").get

    }
  }

  property("Read Order without sender and matcher PublicKey") {
    val json = Json.parse("""
        {
          "senderPublicKey": " ",
          "spendAssetId": "string",
          "receiveAssetId": "string",
          "amount": 0,
          "matcherFee": 0,
          "price": 0,
          "timestamp": 0,
          "expiration": 0,
          "signature": "signature"
        } """)

    json.validate[Order] match {
      case e: JsError =>
        val paths = e.errors.map(_._1)
        paths should contain allOf (JsPath \ "matcherPublicKey", JsPath \ "senderPublicKey")
      case _ =>
        fail("Should be JsError")
    }
  }

  val base58Str = Wallet.generateNewAccount().publicKeyBase58
  val json: JsValue = Json.parse(s"""
    {
      "sender": "$base58Str",
      "wrong_sender": "0abcd",
      "wrong_long": "12e",
      "publicKey": "$base58Str",
      "wrong_publicKey": "0abcd"
    }
    """)

  property("Json Reads Base58") {
    val sender = (json \ "sender").as[Option[Array[Byte]]]
    sender.get shouldBe Base58.decode(base58Str).get

    (json \ "wrong_sender").validate[Array[Byte]] shouldBe a[JsError]
  }

  property("Json Reads PublicKeyAccount") {
    val publicKey = (json \ "publicKey").as[PublicKeyAccount]
    publicKey.publicKeyBase58 shouldBe PublicKeyAccount(Base58.decode(base58Str).get).publicKeyBase58

    (json \ "wrong_publicKey").validate[PublicKeyAccount] match {
      case e: JsError =>
        e.errors.head._2.head.message shouldBe "error.incorrect.publicKeyAccount"
      case _ => fail("Should be JsError")
    }
  }

  property("Parse signed Order") {
    forAll(orderGen) { order =>
      val json = order.json()
      json.validate[Order] match {
        case e: JsError =>
          fail("Error: " + JsError.toJson(e).toString())
        case s: JsSuccess[Order] =>
          val o = s.get
          o.json().toString() should be(json.toString())
          Verifier.verifyAsEllipticCurveSignature(o) shouldBe 'right
      }
    }
  }

  property("Read Order with empty assetId") {
    val pk               = Wallet.generateNewAccount()
    val pubKeyStr        = pk.publicKeyBase58
    val matcherPublicKey = Wallet.generateNewAccount().publicKeyBase58

    val json = Json.parse(s"""
        {
          "senderPublicKey": "$pubKeyStr",
          "matcherPublicKey": "$matcherPublicKey",
           "assetPair": {
             "amountAsset": "",
             "priceAsset": ""
           },
          "orderType": "sell",
          "amount": 0,
          "matcherFee": 0,
          "price": 0,
          "timestamp": 0,
          "expiration": 0,
          "signature": "signature"
        } """)

    json.validate[Order] match {
      case e: JsError =>
        fail("Error: " + JsError.toJson(e).toString())
      case s: JsSuccess[Order] =>
        val o = s.get
        o.assetPair.amountAsset shouldBe empty
        o.assetPair.priceAsset shouldBe empty

    }
  }
}
