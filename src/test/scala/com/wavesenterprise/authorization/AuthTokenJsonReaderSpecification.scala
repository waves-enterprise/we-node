package com.wavesenterprise.authorization

import org.scalatest.{FunSpecLike, Matchers}
import play.api.libs.json.{JsError, JsSuccess, Json}

class AuthTokenJsonReaderSpecification extends FunSpecLike with Matchers {
  it("read AuthToken from json with snake case") {
    val jsonStr =
      """
        |{
        |      "access_token": "asdasd",
        |      "refresh_token": "xxx",
        |      "token_type": "Bearer"
        |}
      """.stripMargin

    val json = Json.parse(jsonStr)
    json.validate[AuthToken] match {
      case JsError(e) =>
        fail("Error: " + e.toString())
      case JsSuccess(authToken, _) =>
        authToken.accessToken shouldBe "asdasd"
        authToken.refreshToken shouldBe "xxx"
    }
  }
}
