package com.wavesenterprise.settings

import cats.Show
import com.wavesenterprise.state.diffs.CommonValidation.MaxTimePrevBlockOverTransactionDiff
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._
import pureconfig.module.squants._
import squants.information.Information

import scala.concurrent.duration.{FiniteDuration, _}

case class UtxSettings(cleanupInterval: FiniteDuration,
                       allowTransactionsFromSmartAccounts: Boolean,
                       memoryLimit: Information,
                       txExpireTimeout: FiniteDuration = MaxTimePrevBlockOverTransactionDiff) {
  require(txExpireTimeout >= (2 hours) && txExpireTimeout <= (96 hours), "txExpireTimeout param should be between 2 and 96 hours")
}

object UtxSettings extends WEConfigReaders {

  implicit val configReader: ConfigReader[UtxSettings] = deriveReader

  implicit val toPrintable: Show[UtxSettings] = { x =>
    import x._

    s"""
       |cleanupInterval: $cleanupInterval
       |allowTransactionsFromSmartAccounts: $allowTransactionsFromSmartAccounts
       |memoryLimit: $memoryLimit
     """.stripMargin // txExpireTimeout param is secret, do not log it anywhere
  }
}
