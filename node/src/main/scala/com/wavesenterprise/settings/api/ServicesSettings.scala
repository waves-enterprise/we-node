package com.wavesenterprise.settings.api

import cats.Show
import cats.syntax.show._
import com.wavesenterprise.settings.WEConfigReaders
import com.wavesenterprise.utils.StringUtilites.dashes
import scala.util.chaining.scalaUtilChainingOps
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

case class ServicesSettings(
    blockchainEvents: BlockchainEventsServiceSettings,
    privacyEvents: PrivacyEventsServiceSettings,
    contractStatusEvents: ContractStatusServiceSettings
)

object ServicesSettings extends WEConfigReaders {
  implicit val configReader: ConfigReader[ServicesSettings] = deriveReader

  implicit val toPrintable: Show[ServicesSettings] = { x =>
    import x._

    s"""
       |blockchainEventsService: ${blockchainEvents.show pipe dashes}
       |privacyEventsService: ${privacyEvents.show pipe dashes}
       |contractStatusEventsService: ${contractStatusEvents.show pipe dashes}
       |""".stripMargin
  }
}
