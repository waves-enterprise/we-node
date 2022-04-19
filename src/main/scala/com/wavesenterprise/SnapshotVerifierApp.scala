package com.wavesenterprise

import java.io.File

import com.wavesenterprise.account.AddressScheme
import com.wavesenterprise.database.rocksdb.{RocksDBStorage, RocksDBWriter}
import com.wavesenterprise.database.snapshot.AsyncSnapshotVerifier
import com.wavesenterprise.settings.ConsensusSettings.PoSSettings
import com.wavesenterprise.settings.FunctionalitySettings
import com.wavesenterprise.utils.{ResourceUtils, ScorexLogging}
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService
import pureconfig.ConfigSource

import scala.concurrent.Await
import scala.concurrent.duration._

object SnapshotVerifierApp extends App with ScorexLogging {

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

  val schedulerName                        = "verifier-pool"
  implicit val scheduler: SchedulerService = Scheduler.computation(name = schedulerName)

  sys.addShutdownHook {
    AppSchedulers.shutdownAndWait(scheduler, schedulerName)
  }

  ResourceUtils.withResource(RocksDBStorage.openDB(snapshotSettings.sourcePath)) { sourceDb =>
    ResourceUtils.withResource(RocksDBStorage.openDB(snapshotSettings.targetPath)) { targetDb =>
      val sourceDbWriter   = new RocksDBWriter(sourceDb, FunctionalitySettings.TESTNET, PoSSettings, 1000, 1000)
      val targetDbWriter   = new RocksDBWriter(targetDb, FunctionalitySettings.TESTNET, PoSSettings, 1000, 1000)
      val snapshotVerifier = new AsyncSnapshotVerifier(sourceDbWriter, targetDbWriter)(scheduler)
      val future           = snapshotVerifier.verify().runAsyncLogErr(scheduler)

      Await.ready(future, 1.hour)
    }
  }

  def exitWithError(errorMessage: String): Nothing = {
    log.error(errorMessage)
    sys.exit(1)
  }
}
