package com.wavesenterprise.settings

import org.scalatest.{FlatSpec, Matchers}
import pureconfig.ConfigSource

class FeaturesSettingsSpecification extends FlatSpec with Matchers {

  "FeaturesSettings" should "read values" in {
    val configSource = ConfigSource.string {
      """
        |{
        |    auto-support-implemented-features = no
        |    auto-shutdown-on-unsupported-feature = yes
        |    supported = [123,124,135]
        |}
      """.stripMargin
    }

    val settings = configSource.loadOrThrow[FeaturesSettings]

    settings.autoSupportImplementedFeatures should be(false)
    settings.autoShutdownOnUnsupportedFeature should be(true)
    settings.featuresSupportMode shouldBe Left(SupportedFeaturesMode.SupportSpecified(List(123, 124, 135)))
  }

  "FeaturesSettings with auto-support-implemented-features = yes" should "get supported from implemented" in {
    val configSource = ConfigSource.string {
      """
       |{
       |    auto-support-implemented-features = yes
       |    auto-shutdown-on-unsupported-feature = yes
       |    supported = [123,124,135]
       |}
      """.stripMargin
    }

    val settings = configSource.loadOrThrow[FeaturesSettings]

    settings.autoSupportImplementedFeatures should be(true)
    settings.autoShutdownOnUnsupportedFeature should be(true)
    settings.featuresSupportMode shouldBe Right(SupportedFeaturesMode.SupportAllImplemented)
  }
}
