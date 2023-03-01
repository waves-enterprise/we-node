package com.wavesenterprise.transaction

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.api.http.SignedDataRequestV3
import com.wavesenterprise.state._
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import play.api.libs.json.{Format, Json}

class DataTransactionV3Specification extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with WithSenderAndRecipient {

  property("JSON roundtrip") {
    implicit val signedFormat: Format[SignedDataRequestV3] = Json.format[SignedDataRequestV3]

    forAll(dataTransactionV3Gen) { tx =>
      val json = tx.json()
      json.toString shouldEqual tx.toString

      val req = json.as[SignedDataRequestV3]
      req.senderPublicKey shouldEqual tx.sender.publicKeyBase58
      req.fee shouldEqual tx.fee
      req.timestamp shouldEqual tx.timestamp

      req.data zip tx.data foreach {
        case (re, te) =>
          re match {
            case BinaryDataEntry(k, v) =>
              k shouldEqual te.key
              v shouldEqual te.value
            case _: DataEntry[_] =>
              re shouldEqual te
            case _ => fail
          }
      }
    }
  }
}
