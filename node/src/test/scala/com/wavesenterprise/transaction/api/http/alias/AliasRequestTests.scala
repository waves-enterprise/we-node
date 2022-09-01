package com.wavesenterprise.transaction.api.http.alias

import com.wavesenterprise.api.http.alias.{CreateAliasV2Request, SignedCreateAliasV2Request}
import com.wavesenterprise.transaction.CreateAliasTransaction
import play.api.libs.json.Json
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AliasRequestTests extends AnyFunSuite with Matchers {
  test("CreateAliasRequest") {
    val json =
      s"""
        {
          "type": ${CreateAliasTransaction.typeId},
          "version": 2,
          "sender": "3Myss6gmMckKYtka3cKCM563TBJofnxvfD7",
          "fee": 10000000,
          "alias": "ALIAS"
        }
      """

    val req = Json.parse(json).validate[CreateAliasV2Request].get

    req shouldBe CreateAliasV2Request(version = 2, "3Myss6gmMckKYtka3cKCM563TBJofnxvfD7", "ALIAS", 10000000)
  }

  test("SignedCreateAliasRequest") {
    val json =
      s"""
         {
           "type": ${CreateAliasTransaction.typeId},
           "version": 2,
           "senderPublicKey": "CRxqEuxhdZBEHX42MU4FfyJxuHmbDBTaHMhM3Uki7pLw",
           "fee": 100000,
           "alias": "ALIAS",
           "timestamp": 1488807184731,
           "proofs": ["3aB6cL1osRNopWyqBYpJQCVCXNLibkwM58dvK85PaTK5sLV4voMhe5E8zEARM6YDHnQP5YE3WX8mxdFp3ciGwVfy"]
          }
       """

    val req = Json.parse(json).validate[SignedCreateAliasV2Request].get

    req shouldBe SignedCreateAliasV2Request(
      version = 2,
      "CRxqEuxhdZBEHX42MU4FfyJxuHmbDBTaHMhM3Uki7pLw",
      100000,
      "ALIAS",
      1488807184731L,
      "3aB6cL1osRNopWyqBYpJQCVCXNLibkwM58dvK85PaTK5sLV4voMhe5E8zEARM6YDHnQP5YE3WX8mxdFp3ciGwVfy" :: Nil
    )
  }
}
