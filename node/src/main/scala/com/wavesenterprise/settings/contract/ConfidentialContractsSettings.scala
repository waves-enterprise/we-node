package com.wavesenterprise.settings.contract

import cats.Show
import cats.implicits.showInterpolator
import com.wavesenterprise.settings.LRUCacheSettings
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

case class ConfidentialContractsSettings(
    dataDirectory: String,
    cache: LRUCacheSettings,
    replier: ConfidentialDataReplierSettings,
    inventoryHandler: ConfidentialInventoryHandlerSettings,
    synchronizer: ConfidentialDataSynchronizerSettings
)

object ConfidentialContractsSettings {
  implicit val configReader: ConfigReader[ConfidentialContractsSettings] = deriveReader

  implicit val toPrintable: Show[ConfidentialContractsSettings] = { s =>
    import s._

    s"""
       |directory: $dataDirectory
       |cache: 
       |  ${show"$cache".replace("\n", "\n--")}
       |replier:
       |  ${show"$replier".replace("\n", "\n--")}
       |inventoryHandler:
       |  ${show"$inventoryHandler".replace("\n", "\n--")}
       |synchronizer:
       |  ${show"$synchronizer".replace("\n", "\n--")}
     """.stripMargin
  }

}
