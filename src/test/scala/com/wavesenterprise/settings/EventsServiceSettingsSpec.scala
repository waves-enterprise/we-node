package com.wavesenterprise.settings

import com.wavesenterprise.settings.api.{
  BlockchainEventsServiceSettings,
  DisabledHistoryEventsBufferSettings,
  EnabledHistoryEventsBufferSettings,
  PrivacyEventsServiceSettings
}
import org.scalatest.{FreeSpec, Matchers}
import pureconfig.ConfigSource
import squants.information.Megabytes

class EventsServiceSettingsSpec extends FreeSpec with Matchers {
  "BlockchainEventsServiceSettings" - {
    "EnabledHistoryEventsBufferSettings should read config string" in {
      val config = ConfigSource.string {
        s"""
           |max-connections = 5
           |history-events-buffer {
           |  enable: true
           |  size-in-bytes: 50MB
           |}
       """.stripMargin
      }

      val expectedSettings = BlockchainEventsServiceSettings(5, EnabledHistoryEventsBufferSettings(Megabytes(50)))
      config.loadOrThrow[BlockchainEventsServiceSettings] shouldBe expectedSettings
    }

    "DisabledHistoryEventsBufferSettings should read config string" in {
      val config = ConfigSource.string {
        s"""
           |max-connections = 5
           |history-events-buffer {
           |  enable: false
           |}
       """.stripMargin
      }
      val expectedSettings = BlockchainEventsServiceSettings(5, DisabledHistoryEventsBufferSettings)
      config.loadOrThrow[BlockchainEventsServiceSettings] shouldBe expectedSettings
    }
  }

  "PrivacyEventsServiceSettings" - {
    "EnabledHistoryEventsBufferSettings should read config string" in {
      val config = ConfigSource.string {
        s"""
           |max-connections = 5
           |history-events-buffer {
           |  enable: true
           |  size-in-bytes: 50MB
           |}
       """.stripMargin
      }

      val expectedSettings = PrivacyEventsServiceSettings(5, EnabledHistoryEventsBufferSettings(Megabytes(50)))
      config.loadOrThrow[PrivacyEventsServiceSettings] shouldBe expectedSettings
    }

    "DisabledHistoryEventsBufferSettings should read config string" in {
      val config = ConfigSource.string {
        s"""
           |max-connections = 5
           |history-events-buffer {
           |  enable: false
           |}
       """.stripMargin
      }
      val expectedSettings = PrivacyEventsServiceSettings(5, DisabledHistoryEventsBufferSettings)
      config.loadOrThrow[PrivacyEventsServiceSettings] shouldBe expectedSettings
    }
  }
}
