package com.wavesenterprise.settings.dockerengine

import cats.Show
import com.wavesenterprise.settings.WEConfigReaders
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

case class ContractExecutionLimitsSettings(startupTimeout: FiniteDuration, timeout: FiniteDuration, memory: Int, memorySwap: Int)

object ContractExecutionLimitsSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[ContractExecutionLimitsSettings] = deriveReader

  implicit val toPrintable: Show[ContractExecutionLimitsSettings] = { x =>
    import x._

    s"""
       |startup-timeout: $startupTimeout
       |timeout:         $timeout
       |memory:          $memory
       |memorySwap:      $memorySwap
       """.stripMargin
  }
}
