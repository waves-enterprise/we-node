package com.wavesenterprise.transaction.api.http.leasing

import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.Json
import com.wavesenterprise.api.http.leasing._
import com.wavesenterprise.transaction.lease.{LeaseCancelTransaction, LeaseTransaction}

class LeaseRequestsTests extends FunSuite with Matchers {

  test("LeaseRequest") {
    val json =
      s"""
        {
          "type": ${LeaseTransaction.typeId},
          "version": 2,
          "amount": 100000,
          "recipient": "3Myss6gmMckKYtka3cKCM563TBJofnxvfD7",
          "sender": "3MwKzMxUKaDaS4CXM8KNowCJJUnTSHDFGMb",
          "fee": 1000
        }
      """

    val req = Json.parse(json).validate[LeaseV2Request].get

    req shouldBe LeaseV2Request(version = 2, "3MwKzMxUKaDaS4CXM8KNowCJJUnTSHDFGMb", 100000, 1000, "3Myss6gmMckKYtka3cKCM563TBJofnxvfD7")
  }

  test("LeaseCancelRequest") {
    val json =
      s"""
        {
          "type": ${LeaseCancelTransaction.typeId},
          "version": 2,
          "sender": "3Myss6gmMckKYtka3cKCM563TBJofnxvfD7",
          "txId": "ABMZDPY4MyQz7kKNAevw5P9eNmRErMutJoV9UNeCtqRV",
          "fee": 10000000
        }
      """

    val req = Json.parse(json).validate[LeaseCancelV2Request].get

    req shouldBe LeaseCancelV2Request(version = 2, "3Myss6gmMckKYtka3cKCM563TBJofnxvfD7", "ABMZDPY4MyQz7kKNAevw5P9eNmRErMutJoV9UNeCtqRV", 10000000)
  }

  test("SignedLeaseRequest") {
    val json =
      s"""
        {
         "type": ${LeaseTransaction.typeId},
         "version": 2,
         "senderPublicKey":"CRxqEuxhdZBEHX42MU4FfyJxuHmbDBTaHMhM3Uki7pLw",
         "recipient":"3MwKzMxUKaDaS4CXM8KNowCJJUnTSHDFGMb",
         "fee":1000000,
         "timestamp":0,
         "amount":100000,
         "proofs":["4VPg4piLZGQz3vBqCPbjTfAR4cDErMi57rDvyith5XrQJDLryU2w2JsL3p4ejEqTPpctZ5YekpQwZPTtYiGo5yPC"]
         }
      """

    val req = Json.parse(json).validate[SignedLeaseV2Request].get

    req shouldBe SignedLeaseV2Request(
      version = 2,
      "CRxqEuxhdZBEHX42MU4FfyJxuHmbDBTaHMhM3Uki7pLw",
      100000L,
      1000000L,
      "3MwKzMxUKaDaS4CXM8KNowCJJUnTSHDFGMb",
      0L,
      "4VPg4piLZGQz3vBqCPbjTfAR4cDErMi57rDvyith5XrQJDLryU2w2JsL3p4ejEqTPpctZ5YekpQwZPTtYiGo5yPC" :: Nil
    )
  }

  test("SignedLeaseCancelRequest") {
    val json =
      s"""
        {
          "type": ${LeaseCancelTransaction.typeId},
          "version": 2,
          "chainId": 84,
         "senderPublicKey":"CRxqEuxhdZBEHX42MU4FfyJxuHmbDBTaHMhM3Uki7pLw",
         "leaseId":"D6HmGZqpXCyAqpz8mCAfWijYDWsPKncKe5v3jq1nTpf5",
         "timestamp":0,
         "fee": 1000000,
         "proofs":["4VPg4piLZGQz3vBqCPbjTfAR4cDErMi57rDvyith5XrQJDLryU2w2JsL3p4ejEqTPpctZ5YekpQwZPTtYiGo5yPC"]
         }
      """

    val req = Json.parse(json).validate[SignedLeaseCancelV2Request].get

    req shouldBe SignedLeaseCancelV2Request(
      version = 2,
      chainId = 'T'.toByte,
      "CRxqEuxhdZBEHX42MU4FfyJxuHmbDBTaHMhM3Uki7pLw",
      "D6HmGZqpXCyAqpz8mCAfWijYDWsPKncKe5v3jq1nTpf5",
      0L,
      "4VPg4piLZGQz3vBqCPbjTfAR4cDErMi57rDvyith5XrQJDLryU2w2JsL3p4ejEqTPpctZ5YekpQwZPTtYiGo5yPC" :: Nil,
      1000000L
    )
  }
}
