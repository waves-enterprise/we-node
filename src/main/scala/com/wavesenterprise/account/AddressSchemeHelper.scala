package com.wavesenterprise.account

import com.typesafe.config.{Config, ConfigFactory}
import com.wavesenterprise.settings.BlockchainSettings.{configPath, mainNetConfResource}
import com.wavesenterprise.settings.BlockchainType
import com.wavesenterprise.settings.WEConfigReaders._

// TODO make implicit?
object AddressSchemeHelper {
  def setAddressSchemaByte(config: Config): Unit = {
    val blockchainType = BlockchainType.withName(config.getString(s"$configPath.type"))

    val configToUse = blockchainType match {
      case BlockchainType.MAINNET => ConfigFactory.parseResources(mainNetConfResource)
      case _                      => config
    }

    val schemaChar = configToUse.as[String](s"$configPath.custom.address-scheme-character").charAt(0)
    AddressScheme.setAddressSchemaByte(schemaChar)
  }
}
