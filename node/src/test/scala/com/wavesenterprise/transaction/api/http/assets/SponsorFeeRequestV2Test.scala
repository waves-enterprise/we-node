package com.wavesenterprise.transaction.api.http.assets

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.api.http.assets.SignedSponsorFeeRequestV2
import com.wavesenterprise.transaction.AtomicBadge
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.wavesenterprise.utils.EitherUtils.EitherExt
import play.api.libs.json.Json

class SponsorFeeRequestV2Test extends AnyFunSuite with Matchers with TransactionGen {

  val acc         = accountGen.sample.get
  val atomicBadge = Some(AtomicBadge(Some(acc.toAddress)))

  test("SponsorFeeRequestV2 json parsing works") {
    val json = s"""{
                |    "version":2,
                |    "senderPublicKey":"AMgSw5s41d9smGTBHKQT6cJ4geXnqfwCz3petMWnhLDA",
                |    "assetId":"HyAkA27DbuhLcFmimoSXoD9H5FP99JX5PcSXvMni4UWM",
                |    "isEnabled": true,
                |    "fee":100000000,
                |    "timestamp":13595396047948,
                |    "atomicBadge": { "trustedSender": "${acc.address}"},
                |    "proofs":[
                |        "4JoAFJNt1j1AGDW5he5jAfvFgcxUu3XE17EZkC9qdsAVhkQ7vaH3YRP21XJ1rFAUVVZvSiBDeo2Eojsqx9Fbp54E"
                |    ]
                |}
                |""".stripMargin

    val req = Json.parse(json).validate[SignedSponsorFeeRequestV2].get
    req.fee shouldBe 100000000L
    req.timestamp shouldBe 13595396047948L
    req.assetId shouldBe "HyAkA27DbuhLcFmimoSXoD9H5FP99JX5PcSXvMni4UWM"
    req.isEnabled shouldBe true
    req.version shouldBe 2
    val tx = req.toTx.explicitGet()
    tx.sender.publicKeyBase58 shouldBe "AMgSw5s41d9smGTBHKQT6cJ4geXnqfwCz3petMWnhLDA"
    tx.fee shouldBe 100000000L
    tx.timestamp shouldBe 13595396047948L
    tx.proofs.proofs.head.base58 shouldBe "4JoAFJNt1j1AGDW5he5jAfvFgcxUu3XE17EZkC9qdsAVhkQ7vaH3YRP21XJ1rFAUVVZvSiBDeo2Eojsqx9Fbp54E"

  }
}
