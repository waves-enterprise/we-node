package com.wavesenterprise.generator

import com.wavesenterprise.settings.FeeSettings
import pureconfig.ConfigSource

package object transaction {
  val fees: FeeSettings.FeesEnabled = {

    ConfigSource
      .string {
        """{
          |    base {
          |      issue = 1 WEST
          |      transfer = 0.1 WEST
          |      reissue = 1 WEST
          |      burn = 1 WEST
          |      exchange = 0.005 WEST
          |      lease = 0.1 WEST
          |      lease-cancel = 0.1 WEST
          |      create-alias = 1 WEST
          |      mass-transfer = 0.1 WEST
          |      data = 0.1 WEST
          |      set-script = 0.5 WEST
          |      sponsor-fee = 1 WEST
          |      set-asset-script = 1 WEST
          |      permit = 0.05 WEST
          |      create-contract = 1 WEST
          |      call-contract = 0.1 WEST
          |      disable-contract = 0.05 WEST
          |      update-contract = 1 WEST
          |      register-node = 0.05 WEST
          |      create-policy = 1 WEST
          |      update-policy = 0.5 WEST
          |      policy-data-hash = 0.1 WEST
          |    }
          |    additional {
          |      mass-transfer = 0.1 WEST
          |      data = 0.05 WEST
          |    }
          |  }
          |""".stripMargin
      }
      .loadOrThrow[FeeSettings.FeesEnabled]

  }
}
