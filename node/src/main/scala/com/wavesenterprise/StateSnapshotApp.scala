package com.wavesenterprise

import java.io.File

import com.wavesenterprise.account.AddressScheme
import com.wavesenterprise.database.rocksdb.RocksDBStorage
import com.wavesenterprise.utils.{ResourceUtils, ScorexLogging}
import pureconfig.generic.semiauto.deriveReader
import pureconfig.{ConfigReader, ConfigSource}

case class SnapshotSettings(sourcePath: String, targetPath: String, chainId: Char)

object SnapshotSettings {
  val configPath: String                                    = "snapshot"
  implicit val configReader: ConfigReader[SnapshotSettings] = deriveReader
}

object StateSnapshotApp extends App with ScorexLogging {

  val configSource = args.headOption.map(new File(_)) match {
    case Some(configFile) if configFile.exists() => ConfigSource.file(configFile)
    case Some(configFile)                        => exitWithError(s"Configuration file '$configFile' does not exist!")
    case None                                    => ConfigSource.empty
  }
  val snapshotSettings = ConfigSource.defaultOverrides
    .withFallback(configSource)
    .at(SnapshotSettings.configPath)
    .loadOrThrow[SnapshotSettings]

  AddressScheme.setAddressSchemaByte(snapshotSettings.chainId)

  ResourceUtils.withResource(RocksDBStorage.openDB(snapshotSettings.sourcePath)) { sourceDb =>
    sourceDb.takeSnapshot(snapshotSettings.targetPath).left.map { ex =>
      log.error(s"Snapshot taking failed with error", ex)
    }
  }

  def exitWithError(errorMessage: String): Nothing = {
    log.error(errorMessage)
    sys.exit(1)
  }
}
