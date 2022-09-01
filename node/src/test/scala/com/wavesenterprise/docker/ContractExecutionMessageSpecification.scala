package com.wavesenterprise.docker

import com.wavesenterprise.account.Address
import com.wavesenterprise.utils.Base58
import org.scalatest.concurrent.Eventually
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import play.api.libs.json.Json
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ContractExecutionMessageSpecification
    extends AnyFreeSpec
    with Matchers
    with ScalaCheckPropertyChecks
    with Eventually
    with ContractExecutionMessageGen {

  "ContractExecutionMessageSpec" - {

    "ContractExecutionMessage bytes serialization roundtrip" in forAll(messageGen()) { message =>
      val restoredMessage = ContractExecutionMessage.parseBytes(message.bytes()).get
      restoredMessage shouldBe message
    }

    "ContractExecutionMessage json format serialization" in forAll(messageGen()) { message =>
      val js = Json.parse(s"""{
                       "sender": "${Address.fromPublicKey(message.rawSenderPubKey).address}",
                       "senderPublicKey": "${Base58.encode(message.rawSenderPubKey)}",
                       "txId": "${message.txId.base58}",
                       "status": "${message.status.toString}",
                       "code": ${message.code.orNull},
                       "message": "${message.message}",
                       "timestamp": ${message.timestamp},
                       "signature": "${message.signature.base58}"
                       }
        """)
      js shouldEqual message.json()
    }
  }
}
