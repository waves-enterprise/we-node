package com.wavesenterprise.settings.api

import cats.Show
import cats.implicits.showInterpolator
import com.wavesenterprise.settings.WEConfigReaders
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._
import pureconfig.module.squants._
import squants.information.Information

trait HistoryEventsBuffer {
  def historyEventsBuffer: HistoryEventsBufferSettings
}

trait MaxConnections {
  def maxConnections: Int
}

trait EventsServiceSettings extends HistoryEventsBuffer with MaxConnections

object EventsServiceSettings {
  def toPrintable(s: EventsServiceSettings): String = {
    import s._
    s"""
       |maxConnections: $maxConnections
       |historyEventsBuffer:
       |  ${show"$historyEventsBuffer".replace("\n", "\n--")}
     """.stripMargin
  }
}

trait HistoryEventsBufferSettings {
  def enable: Boolean
}

object HistoryEventsBufferSettings extends WEConfigReaders {
  implicit val configReader: ConfigReader[HistoryEventsBufferSettings] = ConfigReader.fromCursor { cursor =>
    for {
      objectCursor <- cursor.asObjectCursor
      enableCursor <- objectCursor.atKey("enable")
      isEnable     <- enableCursor.asBoolean
      settings     <- if (isEnable) EnabledHistoryEventsBufferSettings.configReader.from(objectCursor) else Right(DisabledHistoryEventsBufferSettings)
    } yield settings
  }

  implicit val toPrintable: Show[HistoryEventsBufferSettings] = {
    case DisabledHistoryEventsBufferSettings =>
      "enable: false"
    case EnabledHistoryEventsBufferSettings(sizeInBytes) =>
      s"""
         |enable: true
         |sizeInBytes: $sizeInBytes
       """.stripMargin
  }
}

object DisabledHistoryEventsBufferSettings extends HistoryEventsBufferSettings {
  override def enable: Boolean = false
}

case class EnabledHistoryEventsBufferSettings(sizeInBytes: Information) extends HistoryEventsBufferSettings {
  override def enable: Boolean = true
}

object EnabledHistoryEventsBufferSettings extends WEConfigReaders {
  implicit val configReader: ConfigReader[EnabledHistoryEventsBufferSettings] = deriveReader
}
