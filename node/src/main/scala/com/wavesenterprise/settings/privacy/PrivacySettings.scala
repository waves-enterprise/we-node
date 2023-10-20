package com.wavesenterprise.settings.privacy

import cats.Show
import cats.implicits.showInterpolator
import com.wavesenterprise.settings.{LRUCacheSettings, WEConfigReaders}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

case class PrivacySettings(
    replier: PrivacyReplierSettings,
    synchronizer: PrivacySynchronizerSettings,
    inventoryHandler: PrivacyInventoryHandlerSettings,
    cache: LRUCacheSettings,
    storage: PrivacyStorageSettings,
    service: PrivacyServiceSettings
)

object PrivacySettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[PrivacySettings] = deriveReader

  implicit val toPrintable: Show[PrivacySettings] = { s =>
    import s._
    s"""
       |replier:
       |  ${show"$replier".replace("\n", "\n--")}
       |synchronizer:
       |  ${show"$synchronizer".replace("\n", "\n--")}
       |inventoryHandler:
       |  ${show"$inventoryHandler".replace("\n", "\n--")}
       |cache:
       |  ${show"$cache".replace("\n", "\n--")}
       |storage:
       |  ${show"$storage".replace("\n", "\n--")}
       |service:
       |  ${show"$service".replace("\n", "\n--")}
     """.stripMargin
  }
}
