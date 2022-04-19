package com.wavesenterprise.authorization

import akka.grpc.internal.HeaderMetadataImpl
import akka.http.scaladsl.model.headers.{ModeledCustomHeader, ModeledCustomHeaderCompanion}
import com.wavesenterprise.account.Address
import com.wavesenterprise.api.grpc.auth.GrpcAuth
import com.wavesenterprise.api.http.ApiError.InvalidTokenError
import com.wavesenterprise.api.http.auth.AuthRole
import com.wavesenterprise.settings.AuthorizationSettings.OAuth2
import com.wavesenterprise.settings.{AuthorizationSettings, buildSourceBasedOnDefault}
import com.wavesenterprise.utils.Time
import com.wavesenterprise.{CryptoHelpers, TestTime}
import org.scalatest.{FreeSpec, Matchers}
import pureconfig.ConfigSource

import scala.util.Try

class GrpcOauth2AuthorizationSpec extends FreeSpec with Matchers {
  private val testAuth: GrpcAuth = new GrpcAuth {
    override val authSettings: AuthorizationSettings =
      buildSourceBasedOnDefault {
        ConfigSource.string {
          s"""
         |   type: "oauth2"
         |   public-key: "MFswDQYJKoZIhvcNAQEBBQADSgAwRwJAdcOa7wMwxlUpiDMFGHSHNpXNDduWT14LsXaAYj0Pf8DTVibkI380xHwu1yBHM1nVhD55kvQqXkHrSVcO/RDv9QIDAQAB"
         |""".stripMargin
        }
      }.loadOrThrow[AuthorizationSettings]

    override val nodeOwner: Address = CryptoHelpers.generatePrivateKey.toAddress

    override val time: Time = new TestTime
  }

  "should pass with auth of user role if token contains any of the roles" in {
    val token = "Bearer " + "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlcyI6WyJwcml2YWN5IiwiZm9vIl0sImV4cCI6MjU4MDgxODk5OX0.Ghk5PamdYnRNwyN4eoIOjuaHiW1WIWM32mqDYCrdCQ2Hx5qOOstbqBK3c4s0xqENWnbd-u2l6sCbUwKCPvoOGA"
    val meta  = new HeaderMetadataImpl(List(o_auth(token)))
    val auth  = testAuth.withOAuth(meta, testAuth.nodeOwner, testAuth.authSettings.asInstanceOf[OAuth2].publicKey, AuthRole.User)

    auth shouldBe Right(())
  }

  "should pass if token type is not specified" in {
    val token =
      "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlcyI6WyJwcml2YWN5IiwiZm9vIl0sImV4cCI6MjU4MDgxODk5OX0.Ghk5PamdYnRNwyN4eoIOjuaHiW1WIWM32mqDYCrdCQ2Hx5qOOstbqBK3c4s0xqENWnbd-u2l6sCbUwKCPvoOGA"
    val meta = new HeaderMetadataImpl(List(o_auth(token)))
    val auth = testAuth.withOAuth(meta, testAuth.nodeOwner, testAuth.authSettings.asInstanceOf[OAuth2].publicKey, AuthRole.User)

    auth shouldBe Right(())
  }

  "should fail if token type is not specified" in {
    val token = "Sometype " + "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlcyI6WyJwcml2YWN5IiwiZm9vIl0sImV4cCI6MjU4MDgxODk5OX0.Ghk5PamdYnRNwyN4eoIOjuaHiW1WIWM32mqDYCrdCQ2Hx5qOOstbqBK3c4s0xqENWnbd-u2l6sCbUwKCPvoOGA"
    val meta  = new HeaderMetadataImpl(List(o_auth(token)))
    val auth  = testAuth.withOAuth(meta, testAuth.nodeOwner, testAuth.authSettings.asInstanceOf[OAuth2].publicKey, AuthRole.User)

    auth shouldBe Left(InvalidTokenError)
  }
}

object o_auth extends ModeledCustomHeaderCompanion[o_auth] {
  override val name                 = "Authorization"
  override def parse(value: String) = Try(new o_auth(value))
}

final class o_auth(override val value: String) extends ModeledCustomHeader[o_auth] {
  override def companion         = o_auth
  override def renderInRequests  = true
  override def renderInResponses = false
}
