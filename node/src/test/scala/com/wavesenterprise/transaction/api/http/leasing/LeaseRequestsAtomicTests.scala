package com.wavesenterprise.transaction.api.http.leasing

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.api.http.leasing._
import com.wavesenterprise.transaction.AtomicBadge
import com.wavesenterprise.transaction.lease.{LeaseCancelTransaction, LeaseTransaction}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.Json

class LeaseRequestsAtomicTests extends AnyFunSuite with Matchers with TransactionGen {

  val acc         = accountGen.sample.get
  val atomicBadge = Some(AtomicBadge(Some(acc.toAddress)))

  test("LeaseRequest") {
    val json =
      s"""
        {
          "type": ${LeaseTransaction.typeId},
          "version": 3,
          "amount": 100000,
          "recipient": "3Myss6gmMckKYtka3cKCM563TBJofnxvfD7",
          "sender": "3MwKzMxUKaDaS4CXM8KNowCJJUnTSHDFGMb",
          "fee": 1000,
          "atomicBadge": { "trustedSender": "${acc.address}"}
        }
      """

    val req = Json.parse(json).validate[LeaseV3Request].get

    req shouldBe LeaseV3Request(version = 3,
                                "3MwKzMxUKaDaS4CXM8KNowCJJUnTSHDFGMb",
                                100000,
                                1000,
                                "3Myss6gmMckKYtka3cKCM563TBJofnxvfD7",
                                atomicBadge = atomicBadge)
  }

  test("LeaseCancelRequest") {
    val json =
      s"""
        {
          "type": ${LeaseCancelTransaction.typeId},
          "version": 3,
          "sender": "3Myss6gmMckKYtka3cKCM563TBJofnxvfD7",
          "txId": "ABMZDPY4MyQz7kKNAevw5P9eNmRErMutJoV9UNeCtqRV",
          "fee": 10000000,
          "atomicBadge": { "trustedSender": "${acc.address}"}
        }
      """

    val req = Json.parse(json).validate[LeaseCancelV3Request].get

    req shouldBe LeaseCancelV3Request(version = 3,
                                      "3Myss6gmMckKYtka3cKCM563TBJofnxvfD7",
                                      "ABMZDPY4MyQz7kKNAevw5P9eNmRErMutJoV9UNeCtqRV",
                                      10000000,
                                      atomicBadge = atomicBadge)
  }

  test("SignedLeaseRequest") {
    val json =
      s"""
        {
         "type": ${LeaseTransaction.typeId},
         "version": 3,
         "senderPublicKey":"CRxqEuxhdZBEHX42MU4FfyJxuHmbDBTaHMhM3Uki7pLw",
         "recipient":"3MwKzMxUKaDaS4CXM8KNowCJJUnTSHDFGMb",
         "fee":1000000,
         "timestamp":0,
         "amount":100000,
         "atomicBadge": { "trustedSender": "${acc.address}"},
         "proofs":["4VPg4piLZGQz3vBqCPbjTfAR4cDErMi57rDvyith5XrQJDLryU2w2JsL3p4ejEqTPpctZ5YekpQwZPTtYiGo5yPC"]
         }
      """

    val req = Json.parse(json).validate[SignedLeaseV3Request].get

    req shouldBe SignedLeaseV3Request(
      version = 3,
      "CRxqEuxhdZBEHX42MU4FfyJxuHmbDBTaHMhM3Uki7pLw",
      100000L,
      1000000L,
      "3MwKzMxUKaDaS4CXM8KNowCJJUnTSHDFGMb",
      0L,
      proofs = "4VPg4piLZGQz3vBqCPbjTfAR4cDErMi57rDvyith5XrQJDLryU2w2JsL3p4ejEqTPpctZ5YekpQwZPTtYiGo5yPC" :: Nil,
      atomicBadge = atomicBadge
    )
  }

  test("SignedLeaseCancelRequest") {
    val json =
      s"""
        {
          "type": ${LeaseCancelTransaction.typeId},
          "version": 3,
          "chainId": 84,
         "senderPublicKey":"CRxqEuxhdZBEHX42MU4FfyJxuHmbDBTaHMhM3Uki7pLw",
         "leaseId":"D6HmGZqpXCyAqpz8mCAfWijYDWsPKncKe5v3jq1nTpf5",
         "timestamp":0,
         "fee": 1000000,
         "atomicBadge": { "trustedSender": "${acc.address}"},
         "proofs":["4VPg4piLZGQz3vBqCPbjTfAR4cDErMi57rDvyith5XrQJDLryU2w2JsL3p4ejEqTPpctZ5YekpQwZPTtYiGo5yPC"]
         }
      """

    val req = Json.parse(json).validate[SignedLeaseCancelV3Request].get

    req shouldBe SignedLeaseCancelV3Request(
      version = 3,
      chainId = 'T'.toByte,
      "CRxqEuxhdZBEHX42MU4FfyJxuHmbDBTaHMhM3Uki7pLw",
      "D6HmGZqpXCyAqpz8mCAfWijYDWsPKncKe5v3jq1nTpf5",
      0L,
      1000000L,
      proofs = "4VPg4piLZGQz3vBqCPbjTfAR4cDErMi57rDvyith5XrQJDLryU2w2JsL3p4ejEqTPpctZ5YekpQwZPTtYiGo5yPC" :: Nil,
      atomicBadge = atomicBadge
    )
  }
}
