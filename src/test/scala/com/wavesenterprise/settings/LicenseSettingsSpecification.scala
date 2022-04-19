package com.wavesenterprise.settings

import java.io.File

import org.scalatest.{FlatSpec, Matchers}
import pureconfig.ConfigSource

import scala.concurrent.duration._

class LicenseSettingsSpecification extends FlatSpec with Matchers {

  "LicenseSettings" should "read config string correctly" in {
    val configSource = ConfigSource.string {
      """
        |{
        |   file = /dev/null
        |   demo-height = 777
        |}
        |""".stripMargin
    }

    configSource.loadOrThrow[LicenseSettings] shouldBe LicenseSettings(new File("/dev/null"), 777)
  }

  it should s"read demo-height from config less than '${LicenseSettings.MaxDemoHeight}'" in {
    val configSource = ConfigSource.string {
      s"""
        |{
        |   file = /dev/null
        |   demo-height = ${Int.MaxValue}
        |}
      """.stripMargin
    }

    configSource.loadOrThrow[LicenseSettings] shouldBe LicenseSettings(new File("/dev/null"), LicenseSettings.MaxDemoHeight)
  }

  it should "read check-interval correctly" in {
    val configString = ConfigSource.string {
      """
        | {
        |   file = /dev/null
        |   demo-height = 777
        |   check-interval = 11 minutes
        | }
      """.stripMargin
    }

    configString.loadOrThrow[LicenseSettings] shouldBe LicenseSettings(new File("/dev/null"), 777, 11 minutes)
  }

  it should "set check-interval correctly when there's no check-interval param in config" in {
    val configSource = ConfigSource.string {
      """
        | {
        |   file = /dev/null
        |   demo-height = 777
        | }
      """.stripMargin
    }

    configSource.loadOrThrow[LicenseSettings] shouldBe LicenseSettings(new File("/dev/null"), 777, LicenseSettings.CheckIntervalDefault)
  }

  it should s"read check-interval as '${LicenseSettings.CheckIntervalDefault}' when check-interval is too high" in {
    val configSource = ConfigSource.string {
      """
        | {
        |   file = /dev/null
        |   demo-height = 777
        |   check-interval = 2 hours
        | }
      """.stripMargin
    }

    configSource.loadOrThrow[LicenseSettings] shouldBe LicenseSettings(new File("/dev/null"), 777, LicenseSettings.CheckIntervalDefault)
  }

  it should s"read check-interval as '${LicenseSettings.CheckIntervalDefault}' when check-interval is too low" in {
    val configSource = ConfigSource.string {
      """
        | {
        |   file = /dev/null
        |   demo-height = 777
        |   check-interval = 1 second
        | }
      """.stripMargin
    }

    configSource.loadOrThrow[LicenseSettings] shouldBe LicenseSettings(new File("/dev/null"), 777, LicenseSettings.CheckIntervalDefault)
  }
}
