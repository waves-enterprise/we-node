package com.wavesenterprise.transaction.docker

import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.docker.ContractApiVersion
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.{AtomicBadge, Proofs, TransactionParsers}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.apache.commons.codec.digest.DigestUtils
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import play.api.libs.json.Json
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class UpdateContractTransactionV4Spec extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with ContractTransactionGen {

  property("UpdateContractTransactionV4Spec serialization roundtrip") {
    forAll(updateContractV4ParamGen()) { tx =>
      val recovered = UpdateContractTransactionV4.parseBytes(tx.bytes()).get
      recovered shouldBe tx
    }
  }

  property("UpdateContractTransactionV4Spec proto serialization roundtrip") {
    forAll(updateContractV4ParamGen()) { tx =>
      val recovered = UpdateContractTransactionV4.fromProto(tx.toInnerProto).explicitGet()
      recovered shouldBe tx
    }
  }

  property("UpdateContractTransactionV4Spec serialization from TypedTransaction") {
    forAll(updateContractV3ParamGen()) { tx =>
      val recovered = TransactionParsers.parseBytes(tx.bytes()).get
      recovered shouldBe tx
    }
  }

  property("JSON format validation") {
    val timestamp  = System.currentTimeMillis()
    val contractId = "9ekQuYn92natMnMq8KqeGK3Nn7cpKd3BvPEGgD6fFyyz"
    val imageHash  = DigestUtils.sha256Hex("some_data")
    val feeAssetId = "9ekQuYn92natMnMq8KqeGK3Nn7cpKd3BvPEGgD6fFyyz"
    val tx = UpdateContractTransactionV4
      .create(
        PublicKeyAccount(senderAccount.publicKey),
        ByteStr.decodeBase58(contractId).get,
        "localhost:5000/smart-kv",
        imageHash,
        fee = 0,
        timestamp,
        Some(ByteStr.decodeBase58(feeAssetId).get),
        Some(AtomicBadge(Some(senderAccount.toAddress))),
        ValidationPolicy.MajorityWithOneOf(List(senderAccount.toAddress)),
        ContractApiVersion.Initial,
        Proofs(Seq(ByteStr.decodeBase58("32mNYSefBTrkVngG5REkmmGAVv69ZvNhpbegmnqDReMTmXNyYqbECPgHgXrX2UwyKGLFS45j7xDFyPXjF8jcfw94").get))
      )
      .right
      .get

    val js = Json.parse(s"""{
                       "type": 107,
                       "id": "${tx.id()}",
                       "sender": "${senderAccount.address}",
                       "senderPublicKey": "$senderPkBase58",
                       "fee": 0,
                       "timestamp": $timestamp,
                       "proofs": [
                       "32mNYSefBTrkVngG5REkmmGAVv69ZvNhpbegmnqDReMTmXNyYqbECPgHgXrX2UwyKGLFS45j7xDFyPXjF8jcfw94"
                       ],
                       "version": 4,
                       "atomicBadge": { "trustedSender": "${senderAccount.address}"},
                       "validationPolicy": { "type": "majority_with_one_of", "addresses": [ "${senderAccount.address}" ]},
                       "apiVersion": "${tx.apiVersion}",
                       "contractId": "$contractId",
                       "image": "localhost:5000/smart-kv",
                       "imageHash": "$imageHash",
                       "feeAssetId": "$feeAssetId"
                       }
  """)

    js shouldEqual tx.json()
  }
}
