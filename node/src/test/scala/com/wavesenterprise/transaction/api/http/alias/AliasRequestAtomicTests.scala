package com.wavesenterprise.transaction.api.http.alias

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.api.http.alias.{CreateAliasV4Request, SignedCreateAliasV4Request}
import com.wavesenterprise.transaction.{AtomicBadge, CreateAliasTransaction}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.Json

class AliasRequestAtomicTests extends AnyFunSuite with Matchers with TransactionGen {
  test("CreateAliasRequest") {
    val acc         = accountGen.sample.get
    val atomicBadge = Some(AtomicBadge(Some(acc.toAddress)))
    val json =
      s"""
        {
          "type": ${CreateAliasTransaction.typeId},
          "version": 4,
          "sender": "3Myss6gmMckKYtka3cKCM563TBJofnxvfD7",
          "fee": 10000000,
          "atomicBadge": { "trustedSender": "${acc.address}"},
          "alias": "ALIAS"
        }
      """

    val req = Json.parse(json).validate[CreateAliasV4Request].get

    req shouldBe CreateAliasV4Request(version = 4, "3Myss6gmMckKYtka3cKCM563TBJofnxvfD7", "ALIAS", 10000000, atomicBadge = atomicBadge)
  }

  test("SignedCreateAliasRequest") {
    val acc         = accountGen.sample.get
    val atomicBadge = Some(AtomicBadge(Some(acc.toAddress)))
    val json =
      s"""
         {
           "type": ${CreateAliasTransaction.typeId},
           "version": 4,
           "senderPublicKey": "CRxqEuxhdZBEHX42MU4FfyJxuHmbDBTaHMhM3Uki7pLw",
           "fee": 100000,
           "atomicBadge": { "trustedSender": "${acc.address}"},
           "alias": "ALIAS",
           "timestamp": 1488807184731,
           "proofs": ["3aB6cL1osRNopWyqBYpJQCVCXNLibkwM58dvK85PaTK5sLV4voMhe5E8zEARM6YDHnQP5YE3WX8mxdFp3ciGwVfy"]
          }
       """

    val req = Json.parse(json).validate[SignedCreateAliasV4Request].get

    req shouldBe SignedCreateAliasV4Request(
      version = 4,
      "CRxqEuxhdZBEHX42MU4FfyJxuHmbDBTaHMhM3Uki7pLw",
      100000,
      "ALIAS",
      1488807184731L,
      atomicBadge = atomicBadge,
      proofs = "3aB6cL1osRNopWyqBYpJQCVCXNLibkwM58dvK85PaTK5sLV4voMhe5E8zEARM6YDHnQP5YE3WX8mxdFp3ciGwVfy" :: Nil,
      feeAssetId = None
    )
  }
}
