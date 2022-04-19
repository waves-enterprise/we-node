package com.wavesenterprise.http

import java.nio.charset.StandardCharsets.UTF_8

import com.wavesenterprise.crypto
import com.wavesenterprise.settings.{ApiSettings, _}
import com.wavesenterprise.utils.Base58
import pureconfig.ConfigSource

trait ApiSettingsHelper {
  def apiKey: String        = "test_api_key"
  def privacyApiKey: String = "test_privacy_api_key"

  lazy val MaxTransactionsPerRequest = 10000
  lazy val MaxAddressesPerRequest    = 10000

  lazy val restAPISettings: ApiSettings = {
    val keyHash        = Base58.encode(crypto.secureHash(apiKey.getBytes(UTF_8)))
    val privacyKeyHash = Base58.encode(crypto.secureHash(privacyApiKey.getBytes(UTF_8)))

    buildSourceBasedOnDefault {
      ConfigSource
        .string {
          s"""
             |{
             |    rest {
             |      enable = yes
             |      bind-address = "127.0.0.1"
             |      port = 6869
             |      cors = yes
             |      transactions-by-address-limit = $MaxTransactionsPerRequest
             |      distribution-address-limit = $MaxAddressesPerRequest
             |    }
             |
             |    grpc {
             |      enable = yes
             |      bind-address = "127.0.0.2"
             |      port = 6870
             |      services {
             |        blockchain-events {
             |          max-connections = 5
             |          history-events-buffer {
             |            enable: false
             |            size-in-bytes: 50MB
             |          }
             |        }
             |        privacy-events {
             |          max-connections = 5
             |          history-events-buffer {
             |            enable: false
             |            size-in-bytes: 50MB
             |          }
             |        }
             |        contract-status-events {
             |          max-connections = 5
             |        }
             |      }
             |      akka-http-settings {
             |        akka {
             |          http.server.idle-timeout = infinite
             |        }
             |      }
             |    }

             |    auth {
             |      type: "api-key"
             |      api-key-hash = "$keyHash"
             |      privacy-api-key-hash = "$privacyKeyHash"
             |    }
             |}
         |""".stripMargin
        }
    }.loadOrThrow[ApiSettings]
  }
}
