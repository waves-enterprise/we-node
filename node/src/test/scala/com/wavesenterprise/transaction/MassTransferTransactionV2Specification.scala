package com.wavesenterprise.transaction

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.account.PublicKeyAccount
import com.wavesenterprise.api.http.assets.MassTransferRequest
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.utils.Base58
import org.scalatest._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import play.api.libs.json.Json
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class MassTransferTransactionV2Specification extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen {

  property(testName = "JSON format validation") {
    forAll(accountGen) { senderAccount =>
      val transfers = MassTransferRequest
        .parseTransfersList(List(TransferDescriptor(senderAccount.address, 100000000L), TransferDescriptor(senderAccount.address, 200000000L)))
        .right
        .get

      val tx = MassTransferTransactionV2
        .create(
          PublicKeyAccount(senderAccount.publicKey),
          None,
          transfers,
          1518091313964L,
          200000,
          Base58.decode("59QuUcqP6p").get,
          None,
          Proofs(Seq(ByteStr.decodeBase58("FXMNu3ecy5zBjn9b69VtpuYRwxjCbxdkZ3xZpLzB8ZeFDvcgTkmEDrD29wtGYRPtyLS3LPYrL2d5UM6TpFBMUGQ").get))
        )
        .right
        .get

      val js = Json.parse(s"""{
         |  "type": 11,
         |  "id": "${tx.id().base58}",
         |  "sender": "${senderAccount.address}",
         |  "senderPublicKey": "${senderAccount.publicKeyBase58}",
         |  "fee": 200000,
         |  "timestamp": 1518091313964,
         |  "proofs": ["FXMNu3ecy5zBjn9b69VtpuYRwxjCbxdkZ3xZpLzB8ZeFDvcgTkmEDrD29wtGYRPtyLS3LPYrL2d5UM6TpFBMUGQ"],
         |  "version": 2,
         |  "assetId": null,
         |  "attachment": "59QuUcqP6p",
         |  "feeAssetId": null,
         |  "transferCount": 2,
         |  "totalAmount": 300000000,
         |  "transfers": [
         |    {
         |      "recipient": "${senderAccount.address}",
         |      "amount": 100000000
         |    },
         |    {
         |      "recipient": "${senderAccount.address}",
         |      "amount": 200000000
         |    }
         |  ]
         |}""".stripMargin)

      js shouldEqual tx.json()
    }
  }
}
