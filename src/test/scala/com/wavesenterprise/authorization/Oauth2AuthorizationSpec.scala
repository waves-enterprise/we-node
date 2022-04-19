package com.wavesenterprise.authorization

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.http.ApiError.{
  AuthTokenExpired,
  CantParseJwtClaims,
  InvalidNodeOwnerAddress,
  InvalidTokenError,
  MissingAuthorizationMetadata,
  NoRequiredAuthRole
}
import com.wavesenterprise.api.http.auth.{AuthRole, Oauth2Authorization}
import com.wavesenterprise.http.ApiMarshallers
import com.wavesenterprise.settings.{ApiSettings, _}
import com.wavesenterprise.utils.Time
import com.wavesenterprise.{CryptoHelpers, TestTime}
import org.scalatest.{FreeSpec, Matchers}
import play.api.libs.json.JsObject
import pureconfig.ConfigSource

class Oauth2AuthorizationSpec extends FreeSpec with Matchers with ScalatestRouteTest with Directives with ApiMarshallers {

  /**
    * Key pair used in the tests:
    *
    * -----BEGIN PUBLIC KEY-----
    * MFswDQYJKoZIhvcNAQEBBQADSgAwRwJAdcOa7wMwxlUpiDMFGHSHNpXNDduWT14L
    * sXaAYj0Pf8DTVibkI380xHwu1yBHM1nVhD55kvQqXkHrSVcO/RDv9QIDAQAB
    * -----END PUBLIC KEY-----
    *
    * -----BEGIN RSA PRIVATE KEY-----
    * MIIBOQIBAAJAdcOa7wMwxlUpiDMFGHSHNpXNDduWT14LsXaAYj0Pf8DTVibkI380
    * xHwu1yBHM1nVhD55kvQqXkHrSVcO/RDv9QIDAQABAkA2a17UZogKjt4zZ0hKhcba
    * DZ2Fctzh7la++kDXpNndsc7RQxx0cuy5fLdxx91yvWdm98SouyadLkUuG+gsC45h
    * AiEAxl4I+nwfDpbuzkknGVqB5V23HhZ+a1Jit0sZyOnoaS0CIQCX+osg1JiY1NNV
    * w6oVCEQpOOMkvXlLK2CXL6wDtePO6QIhAKVCnEpKc/lMp0E20psduxAihjdL2CCD
    * L3iy2ZV3wcc1AiAGTPfJo6az51bfnl4FwzL4NoiMNGK78A9wFSTffoH0SQIgD5zf
    * h8N7SiwKTWAnXjCxnoGz7iHvMsiK0WKoYa9xCuE=
    * -----END RSA PRIVATE KEY-----
    */
  private val testAuth = new Oauth2Authorization {

    override val settings: ApiSettings =
      buildSourceBasedOnDefault {
        ConfigSource.string {
          s"""
              | node.api.auth {
              |   type: "oauth2"
              |   public-key: "MFswDQYJKoZIhvcNAQEBBQADSgAwRwJAdcOa7wMwxlUpiDMFGHSHNpXNDduWT14LsXaAYj0Pf8DTVibkI380xHwu1yBHM1nVhD55kvQqXkHrSVcO/RDv9QIDAQAB"
              |}
              |""".stripMargin
        }
      }.at("node.api").loadOrThrow[ApiSettings]

    override val time: Time = new TestTime
  }

  "Oauth2 authorization" - {

    "should reject request if missing auth header" in {
      val nodeOwner = CryptoHelpers.generatePrivateKey.toAddress
      val auth      = testAuth.withOAuth(AuthRole.User, nodeOwner)
      HttpRequest() ~> auth(complete(OK)) ~> check {
        status shouldBe MissingAuthorizationMetadata.code
        responseAs[JsObject] shouldBe MissingAuthorizationMetadata.json
      }
    }

    "should reject request if signature is not valid" in {
      val nodeOwner = CryptoHelpers.generatePrivateKey.toAddress
      val auth      = testAuth.withOAuth(AuthRole.User, nodeOwner)
      val testToken = OAuth2BearerToken(
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlcyI6WyJ1c2VyIl0sImV4cCI6MTI4MDgxODk5OX0.WI3FtLJ4OOrcd0M7n_jvE-UWpQ8W4W2rx-6GMiUC4Qg43XLk2JKKV8sdd0IUavzuDDVp4-BhkPtgDL8C994fZ")

      HttpRequest().withHeaders(Authorization(testToken)) ~> auth(complete(OK)) ~> check {
        status shouldBe InvalidTokenError.code
        responseAs[JsObject] shouldBe InvalidTokenError.json
      }
    }

    "should reject request if jwt claim is not valid" in {
      val nodeOwner = CryptoHelpers.generatePrivateKey.toAddress
      val auth      = testAuth.withOAuth(AuthRole.User, nodeOwner)
      val testToken = OAuth2BearerToken(
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJmb28iOlsiYmFyIl0sImV4cCI6MjU4MDgxODk5OX0.YP6wbCu77HaySiOX1ir-R_OfjTCTT09uXS2WeomEuJNhFyX-XL86aA6SXoOvzVovk-aoy5n2Qfl66Zd_0v8a3A")

      HttpRequest().withHeaders(Authorization(testToken)) ~> auth(complete(OK)) ~> check {
        status shouldBe CantParseJwtClaims.code
        responseAs[JsObject] shouldBe CantParseJwtClaims.json
      }
    }

    "should reject request if token expired" in {
      val nodeOwner = CryptoHelpers.generatePrivateKey.toAddress
      val auth      = testAuth.withOAuth(AuthRole.User, nodeOwner)
      val testToken = OAuth2BearerToken(
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlcyI6WyJ1c2VyIl0sImV4cCI6MTI4MDgxODk5OX0.WI3FtLJ4OOrcd0M7n_jvE-UWpQ8W4W2rx-6GMiUC4Qg43XLk2JKKV8sdd0IUavzuDDVp4-BhkPtgDL8C994fZQ")

      HttpRequest().withHeaders(Authorization(testToken)) ~> auth(complete(OK)) ~> check {
        status shouldBe AuthTokenExpired.code
        responseAs[JsObject] shouldBe AuthTokenExpired.json
      }
    }

    "should reject request with auth of admin role if token does not contain admin role" in {
      val nodeOwner = CryptoHelpers.generatePrivateKey.toAddress
      val auth      = testAuth.withOAuth(AuthRole.Administrator, nodeOwner)
      val testToken = OAuth2BearerToken(
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlcyI6WyJ1c2VyIl0sImV4cCI6MjI4MDgxODk5OX0.VGqOOeoCbIzSPCLLBNqbXHl8QfR3rfP01J5ADd888cNPELsPNBi10aTIUji2JLqTLeJU0faRm-rVWkp9UMW0Dw")

      HttpRequest().withHeaders(Authorization(testToken)) ~> auth(complete(OK)) ~> check {
        val expectedError = NoRequiredAuthRole(AuthRole.Administrator)
        status shouldBe expectedError.code
        responseAs[JsObject] shouldBe expectedError.json
      }
    }

    "should reject request with auth of admin role if token does not contain admin role and contains an unknown role" in {
      val nodeOwner = CryptoHelpers.generatePrivateKey.toAddress
      val auth      = testAuth.withOAuth(AuthRole.Administrator, nodeOwner)
      val testToken = OAuth2BearerToken(
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlcyI6WyJ1c2VyIiwiZm9vIl0sImV4cCI6MjI4MDgxODk5OX0.abIKEcYBcrxUzHfTCygQSMo3cCNkv6KChN_xuzI-9z5agQg9rEPrvw2TdjgylWHSl1ZJrvBGDG5ziKXp_Is-Ew")

      HttpRequest().withHeaders(Authorization(testToken)) ~> auth(complete(OK)) ~> check {
        val expectedError = NoRequiredAuthRole(AuthRole.Administrator)
        status shouldBe expectedError.code
        responseAs[JsObject] shouldBe expectedError.json
      }
    }

    "should pass request with auth of user role if token contains any of the roles" in {
      val nodeOwner = CryptoHelpers.generatePrivateKey.toAddress
      val auth      = testAuth.withOAuth(AuthRole.User, nodeOwner)
      val testToken = OAuth2BearerToken(
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlcyI6WyJwcml2YWN5IiwiZm9vIl0sImV4cCI6MjU4MDgxODk5OX0.Ghk5PamdYnRNwyN4eoIOjuaHiW1WIWM32mqDYCrdCQ2Hx5qOOstbqBK3c4s0xqENWnbd-u2l6sCbUwKCPvoOGA")

      HttpRequest().withHeaders(Authorization(testToken)) ~> auth(complete(OK)) ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    "should pass request with auth of user role if token contains user role and an unknown role" in {
      val nodeOwner = CryptoHelpers.generatePrivateKey.toAddress
      val auth      = testAuth.withOAuth(AuthRole.User, nodeOwner)
      val testToken = OAuth2BearerToken(
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlcyI6WyJwcml2YWN5Il0sImV4cCI6MjU4MDgxODk5OX0.Uqxwwd3uTMYye_SL2m95lZ9Y2PLBCgiEQmc2AFwBDEql_lUiEwfy6ar7xF3-E9LHVd-Rsh9yPjBag3GI3ZDIYQ")

      HttpRequest().withHeaders(Authorization(testToken)) ~> auth(complete(OK)) ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    "should pass request with auth of admin role if token contains admin role" in {
      val nodeOwner = Address.fromString("3MqQQYcdzYRGB1Dv5rsMU1DWRCbjNz1zTAs").right.get
      val auth      = testAuth.withOAuth(AuthRole.Administrator, nodeOwner)
      val testToken = OAuth2BearerToken(
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlcyI6WyJhZG1pbiJdLCJhZGRyZXNzZXMiOlt7ImFkZHJlc3MiOiIzTXFRUVljZHpZUkdCMUR2NXJzTVUxR" +
          "FdSQ2JqTnoxelRBcyIsInR5cGUiOiJub2RlIn1dLCJleHAiOjI1ODA4MTg5OTl9.Cc3VWMxI6vFN95Z7E_UGJA_FDu_gL3KvRCeOmHTN43zKwZYV2MA-S9cJB1dJ0fayDJs6Xm5h2FC_GjelBAoaXA")

      HttpRequest().withHeaders(Authorization(testToken)) ~> auth(complete(OK)) ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    "should reject request with auth of admin role and specified node owner if token does not contain specified node owner" in {
      val nodeOwner = CryptoHelpers.generatePrivateKey.toAddress
      val auth      = testAuth.withOAuth(AuthRole.Administrator, nodeOwner)
      val testToken = OAuth2BearerToken(
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlcyI6WyJhZG1pbiJdLCJleHAiOjI1ODA4MTg5OTl9.AUe_sLqNmEhjWLDmuCXNGf_gTez1XrwoSoMaDpWV5tAFkRVDmRhViuqPo0PIs5tARtiWGiWKRmAxltVn4Nat7Q")

      HttpRequest().withHeaders(Authorization(testToken)) ~> auth(complete(OK)) ~> check {
        status shouldBe InvalidNodeOwnerAddress.code
        responseAs[JsObject] shouldBe InvalidNodeOwnerAddress.json
      }
    }

    "should reject request with auth of privacy role and specified node owner if token does not contain privacy role" in {
      val nodeOwner = Address.fromString("3MqQQYcdzYRGB1Dv5rsMU1DWRCbjNz1zTAs").right.get
      val auth      = testAuth.withOAuth(AuthRole.PrivacyUser, nodeOwner)
      val testToken = OAuth2BearerToken(
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlcyI6WyJ1c2VyIl0sImFkZHJlc3NlcyI6W3siYWRkcmVzcyI6IjNNcVFRWWNkellSR0IxRHY1cnNNVTFEV1JDYmpOejF6VEFzIiwidHlwZSI6Im5vZGUifV0sImV4cCI6MjU" +
          "4MDgxODk5OX0.KIvMEW40l46vlnQh7qfUm1ZcvSCG2tNBLya1Sm4vJVsBA6eTH3dq8CHvYhKqlR3gHk90fnp5f3D2mPjcCI0EvQ")

      HttpRequest().withHeaders(Authorization(testToken)) ~> auth(complete(OK)) ~> check {
        val expectedError = NoRequiredAuthRole(AuthRole.PrivacyUser)
        status shouldBe expectedError.code
        responseAs[JsObject] shouldBe expectedError.json
      }
    }

    "should pass request with auth of privacy role and specified node owner if token contains privacy role and specified node owner" in {
      val nodeOwner = Address.fromString("3MqQQYcdzYRGB1Dv5rsMU1DWRCbjNz1zTAs").right.get
      val auth      = testAuth.withOAuth(AuthRole.PrivacyUser, nodeOwner)
      val testToken = OAuth2BearerToken(
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlcyI6WyJwcml2YWN5Il0sImFkZHJlc3NlcyI6W3siYWRkcmVzcyI6IjNNcVFRWWNkellSR0IxRHY1cnNNVTFEV1JDYmpOejF6VEFzIiwidHlwZSI6Im5vZGUifV0sImV4cCI6" +
          "MjU4MDgxODk5OX0.McsE5hP-z4XlzAd5eS94SBcWC-DEtR6UOopvrgccPwRLMIahNeHDcYkwC6YijPeUEQ1fzs8weU5K-hA42D-YxQ"
      )

      HttpRequest().withHeaders(Authorization(testToken)) ~> auth(complete(OK)) ~> check {
        status shouldBe StatusCodes.OK
      }
    }
  }
}
