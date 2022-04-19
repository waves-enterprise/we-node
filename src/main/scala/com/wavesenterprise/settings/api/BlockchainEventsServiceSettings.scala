package com.wavesenterprise.settings.api

import cats.Show
import com.wavesenterprise.settings.WEConfigReaders
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

case class BlockchainEventsServiceSettings(maxConnections: Int, historyEventsBuffer: HistoryEventsBufferSettings) extends EventsServiceSettings

object BlockchainEventsServiceSettings extends WEConfigReaders {
  implicit val configReader: ConfigReader[BlockchainEventsServiceSettings] = deriveReader

  implicit val toPrintable: Show[BlockchainEventsServiceSettings] = EventsServiceSettings.toPrintable(_)
}
