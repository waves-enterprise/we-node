package com.wavesenterprise.settings

import java.nio.charset.StandardCharsets.UTF_8
import java.security.KeyPairGenerator
import com.typesafe.config.ConfigFactory
import com.wavesenterprise.settings.api.{
  BlockchainEventsServiceSettings,
  ContractStatusServiceSettings,
  DisabledHistoryEventsBufferSettings,
  EnabledHistoryEventsBufferSettings,
  PrivacyEventsServiceSettings,
  ServicesSettings
}
import com.wavesenterprise.state.ByteStr
import com.wavesenterprise.utils.{Base58, Base64}
import pureconfig.ConfigSource
import squants.information.Megabytes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.error.ConfigReaderException

class ApiSettingsSpecification extends AnyFlatSpec with Matchers {

  private val restConfigStr =
    """
      |rest {
      |  enable: yes
      |  bind-address: "127.0.0.1"
      |  port: 6869
      |  cors: yes
      |  transactions-by-address-limit = 10000
      |  distribution-address-limit = 10000
      |}""".stripMargin

  private val akkaConfigStr =
    """
      |akka {
      |  http.server.idle-timeout = infinite
      |}""".stripMargin

  private val servicesConfigStr =
    """
      |blockchain-events {
      |  max-connections = 17
      |  history-events-buffer {
      |    enable: false
      |    size-in-bytes: 4MB
      |  }
      |}
      |privacy-events {
      |  max-connections = 4
      |  history-events-buffer {
      |    enable: true
      |    size-in-bytes: 49MB
      |  }
      |}
      |contract-status-events {
      |  max-connections = 8
      |}
      |""".stripMargin

  private val grpcConfigStr =
    s"""
      |grpc {
      |  enable: yes
      |  bind-address: "127.0.0.2"
      |  port: 6870
      |  services: {
      |    $servicesConfigStr
      |  }
      |  akka-http-settings {
      |    $akkaConfigStr
      |  }
      |}""".stripMargin

  "ApiSettings" should "read values (api-key auth)" in {
    val apiKeyHashBytes                      = "BASE58APIKEYHASH".getBytes(UTF_8)
    val privacyApiKeyHashBytes               = "BASE58PRIVACYAPIKEYHASH".getBytes(UTF_8)
    val confidentialContractsApiKeyHashBytes = "BASE58CONFIDENTIALCONTRACTS".getBytes(UTF_8)

    val apiKeyHashBase58                      = Base58.encode(apiKeyHashBytes)
    val privacyApiKeyHashBase58               = Base58.encode(privacyApiKeyHashBytes)
    val confidentialContractsApiKeyHashBase58 = Base58.encode(confidentialContractsApiKeyHashBytes)

    val configSource = ConfigSource.string {
      s"""
         |{
         |  $restConfigStr
         |  $grpcConfigStr
         |
         |  auth {
         |    type: "api-key"
         |    api-key-hash: "$apiKeyHashBase58"
         |    privacy-api-key-hash: "$privacyApiKeyHashBase58"
         |    confidential-contracts-api-key-hash: "$confidentialContractsApiKeyHashBase58"
         |  }
         |}
      """.stripMargin
    }

    val settings = configSource.loadOrThrow[ApiSettings]

    settings.rest.enable shouldBe true
    settings.rest.bindAddress shouldBe "127.0.0.1"
    settings.rest.port shouldBe 6869
    settings.rest.cors shouldBe true
    settings.rest.transactionsByAddressLimit shouldBe 10000
    settings.rest.distributionAddressLimit shouldBe 10000

    settings.grpc.enable shouldBe true
    settings.grpc.bindAddress shouldBe "127.0.0.2"
    settings.grpc.port shouldBe 6870
    settings.grpc.akkaHttpSettings shouldBe ConfigFactory.parseString(akkaConfigStr)

    settings.auth match {
      case apiKeyAuth: AuthorizationSettings.ApiKey =>
        apiKeyAuth.apiKeyHashBytes should contain theSameElementsAs apiKeyHashBytes
        apiKeyAuth.privacyApiKeyHashBytes should contain theSameElementsAs privacyApiKeyHashBytes

      case other =>
        fail(s"Expected authorization settings to be ApiKey, but got $other")
    }
  }

  it should "not read values with empty api-key" in {
    val apiKeyIsEmptyMessage = "requirement failed: " + AuthorizationSettings.ApiKey.API_KEY_EMPTY_MESSAGE
    val configSource = ConfigSource.string {
      s"""
         |{
         |  $restConfigStr
         |  $grpcConfigStr
         |
         |  auth {
         |    type: "api-key"
         |    api-key-hash: ""
         |    privacy-api-key-hash: ""
         |    confidential-contracts-api-key-hash: ""
         |  }
         |}
      """.stripMargin
    }

    the[IllegalArgumentException] thrownBy configSource.loadOrThrow[ApiSettings] should have message apiKeyIsEmptyMessage
  }

  it should "not read values with empty privacy-api-key" in {
    val apiKeyHashBytes      = "BASE58APIKEYHASH".getBytes(UTF_8)
    val apiKeyHashBase58     = Base58.encode(apiKeyHashBytes)
    val apiKeyIsEmptyMessage = "requirement failed: " + AuthorizationSettings.ApiKey.PRIVACY_API_KEY_EMPTY_MESSAGE
    val configSource = ConfigSource.string {
      s"""
         |{
         |  $restConfigStr
         |  $grpcConfigStr
         |
         |  auth {
         |    type: "api-key"
         |    api-key-hash: "$apiKeyHashBase58"
         |    privacy-api-key-hash: ""
         |    confidential-contracts-api-key-hash: "$apiKeyHashBase58"
         |  }
         |}
      """.stripMargin
    }

    the[IllegalArgumentException] thrownBy configSource.loadOrThrow[ApiSettings] should have message apiKeyIsEmptyMessage
  }

  it should "read values (oauth2)" in {
    val oauthPublicKey = {
      val keyGen  = KeyPairGenerator.getInstance("RSA")
      val keyPair = keyGen.generateKeyPair()
      keyPair.getPublic
    }

    val publicKeyEncoded     = oauthPublicKey.getEncoded
    val oauthPublicKeyBase64 = Base64.encode(publicKeyEncoded)

    val configSource = ConfigSource.string {
      s"""
         |{
         |  $restConfigStr
         |  $grpcConfigStr
         |
         |  auth {
         |    type: "oauth2"
         |    public-key: "$oauthPublicKeyBase64"
         |  }
         |}
      """.stripMargin
    }

    val settings = configSource.loadOrThrow[ApiSettings]

    settings.rest.enable shouldBe true
    settings.rest.bindAddress shouldBe "127.0.0.1"
    settings.rest.port shouldBe 6869
    settings.rest.cors shouldBe true
    settings.rest.transactionsByAddressLimit shouldBe 10000
    settings.rest.distributionAddressLimit shouldBe 10000

    settings.grpc.enable shouldBe true
    settings.grpc.bindAddress shouldBe "127.0.0.2"
    settings.grpc.port shouldBe 6870

    {
      val servicesExpected: ServicesSettings = ServicesSettings(
        BlockchainEventsServiceSettings(17, DisabledHistoryEventsBufferSettings),
        PrivacyEventsServiceSettings(4, EnabledHistoryEventsBufferSettings(Megabytes(49))),
        ContractStatusServiceSettings(8)
      )

      settings.grpc.services shouldBe servicesExpected
    }

    settings.grpc.akkaHttpSettings shouldBe ConfigFactory.parseString(akkaConfigStr)

    settings.auth match {
      case apiKeyAuth: AuthorizationSettings.OAuth2 =>
        apiKeyAuth.authServicePublicKey.getEncoded should contain theSameElementsAs publicKeyEncoded

      case other =>
        fail(s"Expected authorization settings to be OAuth2, but got $other")
    }
  }

  it should "read values (tls-whitelist)" in {
    val correctPublicKey: ByteStr =
      ByteStr
        .decodeBase64(
          "MGYwHwYIKoUDBwEBAQEwEwYHKoUDAgIkAAYIKoUDBwEBAgIDQwAEQLh7lrv/ioWdnUkvX3NybRoeew1PPz1vMaajxzpi1CYoWRlrPC9RjlVul5PCMyAL2OuO4lgpcqBj1+y2cEjajXw=")
        .get

    val configSource = ConfigSource.string {
      s"""
         |{
         |  $restConfigStr
         |  $grpcConfigStr
         |
         |  auth {
         |    type: "tls-whitelist"
         |    admin-public-keys: [
         |      "${correctPublicKey.base64}"
         |    ]
         |  }
         |}
         |""".stripMargin
    }

    val settings = configSource.loadOrThrow[ApiSettings]

    settings.auth match {
      case AuthorizationSettings.TlsWhitelist(adminPublicKeys) =>
        assert(adminPublicKeys.contains(correctPublicKey), "adminPublicKeys should contain predefined public key")
      case other =>
        fail(s"Expected authorization settings to be TlsWhitelist, but got $other")
    }
  }

  it should "not read TlsWhitelist auth with empty admin-public-keys" in {
    val incorrectPublicKey =
      "incorrectBase64PublicKey1234567890-=//"

    val configSource = ConfigSource.string {
      s"""
         |{
         |  $restConfigStr
         |  $grpcConfigStr
         |
         |  auth {
         |    type: "tls-whitelist"
         |    admin-public-keys: [
         |      "$incorrectPublicKey"
         |    ]
         |  }
         |}
         |""".stripMargin
    }

    intercept[ConfigReaderException[_]] {
      configSource.loadOrThrow[ApiSettings]
    }
  }
}
