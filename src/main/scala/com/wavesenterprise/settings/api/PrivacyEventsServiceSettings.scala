package com.wavesenterprise.settings.api

import cats.Show
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

case class PrivacyEventsServiceSettings(maxConnections: Int, historyEventsBuffer: HistoryEventsBufferSettings) extends EventsServiceSettings

object PrivacyEventsServiceSettings {
  implicit val configReader: ConfigReader[PrivacyEventsServiceSettings] = deriveReader

  implicit val toPrintable: Show[PrivacyEventsServiceSettings] = EventsServiceSettings.toPrintable(_)
}
