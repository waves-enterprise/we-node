package com.wavesenterprise.authorization

import com.wavesenterprise.CryptoHelpers
import com.wavesenterprise.api.http.auth.{AddressEntry, AuthRole, JwtContent}
import play.api.libs.json.{JsError, JsSuccess, Json}
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

class JwtContentSpec extends AnyFunSpecLike with Matchers {

  private val randomAddresses = Seq.tabulate(3)(_ => AddressEntry(CryptoHelpers.generatePrivateKey.toAddress))

  it("read JwtContent from json") {
    val jsonStr =
      s"""
        |{
        |  "roles": [
        |    "user",
        |    "admin"
        |  ],
        |  "addresses": [
        |     {
        |        "address": "${randomAddresses.head.address}",
        |        "type": "${randomAddresses.head.`type`}",
        |        "name": ""
        |     },
        |    {
        |        "address": "${randomAddresses(1).address}",
        |        "type": "${randomAddresses(1).`type`}",
        |        "name": ""
        |     },
        |     {
        |        "address": "${randomAddresses(2).address}",
        |        "type": "${randomAddresses(2).`type`}",
        |        "name": ""
        |     }
        |  ]
        |}
      """.stripMargin

    val json = Json.parse(jsonStr)
    json.validate[JwtContent] match {
      case JsError(e) =>
        fail("Error: " + e.toString())
      case JsSuccess(jwtContent, _) =>
        jwtContent.roles shouldBe Set(AuthRole.User, AuthRole.Administrator)
        jwtContent.addresses shouldBe randomAddresses.toSet
    }
  }

  it("read JwtContent from json without node owners") {
    val jsonStr =
      """
        |{
        |  "roles": [
        |    "user",
        |    "admin"
        |  ]
        |}
      """.stripMargin

    val json = Json.parse(jsonStr)
    json.validate[JwtContent] match {
      case JsError(e) =>
        fail("Error: " + e.toString())
      case JsSuccess(jwtContent, _) =>
        jwtContent.roles shouldBe Set(AuthRole.User, AuthRole.Administrator)
        jwtContent.addresses shouldBe Set.empty
    }
  }

  it("read JwtContent from json without node owners that has unknown roles") {
    val jsonStr =
      """
        |{
        |  "roles": [
        |    "user",
        |    "admin",
        |    "unknown role1",
        |    "unknown role2"
        |  ]
        |}
      """.stripMargin

    val json = Json.parse(jsonStr)
    json.validate[JwtContent] match {
      case JsError(e) =>
        fail("Error: " + e.toString())
      case JsSuccess(jwtContent, _) =>
        jwtContent.roles shouldBe Set(AuthRole.User, AuthRole.Administrator)
        jwtContent.addresses shouldBe Set.empty
    }
  }

  it("read JwtContent ignoring invalid addresses and addresses from other networks") {
    val addressFromAnotherNetwork = CryptoHelpers.generatePrivateKey.toAddress(Byte.MinValue).address
    val correctAddress            = randomAddresses(2).address.address
    val jsonStr =
      s"""
        |{
        |  "roles": [
        |    "user",
        |    "admin"
        |  ],
        |  "addresses": [
        |     {
        |        "address": "$addressFromAnotherNetwork",
        |        "type": "client",
        |        "name": ""
        |     },
        |    {
        |        "address": "INVALIDADDRESS",
        |        "type": "node",
        |        "name": ""
        |     },
        |     {
        |        "address": "$correctAddress",
        |        "type": "somethingelse",
        |        "name": ""
        |     }
        |  ]
        |}
        |""".stripMargin

    val json = Json.parse(jsonStr)
    json.validate[JwtContent] match {
      case JsError(e) =>
        fail(s"Error: $e")

      case JsSuccess(jwtContent, _) =>
        jwtContent.roles shouldBe Set(AuthRole.User, AuthRole.Administrator)
        jwtContent.addresses.size shouldBe 1
        jwtContent.addresses.head.address.address shouldBe correctAddress
    }
  }

}
