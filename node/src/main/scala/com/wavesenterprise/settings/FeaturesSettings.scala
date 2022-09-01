package com.wavesenterprise.settings

import cats.Show
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._
import com.wavesenterprise.settings.SupportedFeaturesMode.{SupportAllImplemented, SupportSpecified}

case class FeaturesSettings(
    autoSupportImplementedFeatures: Boolean,
    autoShutdownOnUnsupportedFeature: Boolean,
    supported: List[Short]
) {
  def featuresSupportMode: Either[SupportSpecified, SupportAllImplemented.type] =
    Either.cond(autoSupportImplementedFeatures, SupportAllImplemented, SupportSpecified(supported))
}

object FeaturesSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[FeaturesSettings] = deriveReader

  implicit val toPrintable: Show[FeaturesSettings] = { x =>
    import x._

    s"""
       |autoSupportImplementedFeatures: $autoSupportImplementedFeatures
       |autoShutdownOnUnsupportedFeature: $autoShutdownOnUnsupportedFeature
       |featuresSupportMode: ${featuresSupportMode.merge}
     """.stripMargin
  }
}

sealed trait SupportedFeaturesMode

object SupportedFeaturesMode {
  case object SupportAllImplemented                           extends SupportedFeaturesMode
  case class SupportSpecified(supportedFeatures: List[Short]) extends SupportedFeaturesMode
}
