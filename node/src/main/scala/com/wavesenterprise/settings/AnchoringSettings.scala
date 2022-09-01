package com.wavesenterprise.settings

import akka.http.scaladsl.model.Uri
import cats.Show
import cats.implicits.showInterpolator
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import scala.concurrent.duration.FiniteDuration

/**
  * Anchoring settings.
  * Can be either disabled or enabled.
  */
sealed trait AnchoringSettings {
  def enable: Boolean
}

object AnchoringSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[AnchoringSettings] = ConfigReader.fromCursor { cursor =>
    for {
      objectCursor <- cursor.asObjectCursor
      enableCursor <- objectCursor.atKey("enable")
      isEnable     <- enableCursor.asBoolean
      settings     <- if (isEnable) AnchoringEnableSettings.configReader.from(objectCursor) else Right(AnchoringDisableSettings)
    } yield settings
  }

  implicit val toPrintable: Show[AnchoringSettings] = {
    case AnchoringDisableSettings => "enable: false"
    case as: AnchoringEnableSettings =>
      s"""
         |sidechainFee: ${as.sidechainFee}
         |heightCondition:
         |  ${show"${as.heightCondition}".replace("\n", "\n--")}
         |threshold: ${as.threshold}
         |targetnetSettings:
         |  ${show"${as.targetnet}".replace("\n", "\n--")}
       """.stripMargin
  }
}

case object AnchoringDisableSettings extends AnchoringSettings {
  val enable = false
}

case class HeightCondition(range: Int, above: Int)

object HeightCondition extends WEConfigReaders {

  implicit val configReader: ConfigReader[HeightCondition] = deriveReader

  implicit val toPrintable: Show[HeightCondition] = { x =>
    import x._

    s"""
       |heightRange: $range
       |heightAbove: $above
       """.stripMargin
  }
}

case class AnchoringEnableSettings(
    heightCondition: HeightCondition,
    threshold: Int,
    targetnet: TargetnetSettings,
    sidechainFee: Long,
    txMiningCheckDelay: Option[FiniteDuration],
    txMiningCheckCount: Option[Int]
) extends AnchoringSettings {
  override val enable = true
}

object AnchoringEnableSettings extends WEConfigReaders {
  implicit val configReader: ConfigReader[AnchoringEnableSettings] = deriveReader
}

case class TargetnetSettings(
    schemeByte: String,
    nodeAddress: Uri,
    auth: TargetnetAuthSettings,
    nodeRecipientAddress: String,
    privateKeyPassword: Option[String],
    wallet: WalletSettings,
    fee: Long
) {
  val numericSchemeByte: Byte = schemeByte.head.toByte
}

object TargetnetSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[TargetnetSettings] = deriveReader

  implicit val toPrintable: Show[TargetnetSettings] = { x =>
    import x._

    s"""
       |targetnetSchemeByte: $schemeByte
       |targetnetNodeApiUrl: $nodeAddress
       |targetnetRecipientAddress: $nodeRecipientAddress
       |targetnetFee: $fee
       |targetnetNodeAuth:
       |  ${show"$auth".replace("\n", "\n--")}
       |targetnetWalletSettings:
       |  ${show"$wallet".replace("\n", "\n--")}
     """.stripMargin
  }
}
