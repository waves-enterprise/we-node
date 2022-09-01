package com.wavesenterprise.authorization

import play.api.libs.json.{JsError, JsSuccess, Json}
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

class AuthTokenJsonReaderSpecification extends AnyFunSpecLike with Matchers {
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
