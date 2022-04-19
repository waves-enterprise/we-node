package com.wavesenterprise.database.snapshot

import com.wavesenterprise.ShutdownMode
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.api.http.ApiRoute
import com.wavesenterprise.api.http.service.PeersIdentityService
import com.wavesenterprise.api.http.snapshot.{DisabledSnapshotApiRoute, EnabledSnapshotApiRoute, SnapshotApiRoute}
import com.wavesenterprise.database.rocksdb.{DefaultReadOnlyParams, RocksDBStorage, RocksDBWriter}
import com.wavesenterprise.database.snapshot.PackedSnapshot._
import com.wavesenterprise.network.peers.ActivePeerConnections
import com.wavesenterprise.network.snapshot.GenesisSnapshotSource
import com.wavesenterprise.network.{ChannelObservable, GenesisSnapshotRequest, SnapshotNotification, SnapshotRequest}
import com.wavesenterprise.settings.{ConsensusSettings, FunctionalitySettings, WESettings}
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.transaction.BlockchainUpdater
import com.wavesenterprise.utils.Time
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observer
import monix.reactive.subjects.ConcurrentSubject

import scala.concurrent.duration._

trait SnapshotComponents {
  def snapshotApiRoute: SnapshotApiRoute
  def routes: Seq[ApiRoute] = Seq(snapshotApiRoute)
  def close(mode: ShutdownMode.Mode): Unit
}

case class DisabledSnapshotComponents(
    snapshotApiRoute: DisabledSnapshotApiRoute,
    genesisSnapshotSource: GenesisSnapshotSource
) extends SnapshotComponents {
  override def close(mode: ShutdownMode.Mode): Unit =
    genesisSnapshotSource.close()
}

class EnabledSnapshotComponents(consensualSnapshot: ConsensualSnapshot,
                                snapshotBroadcaster: SnapshotBroadcaster,
                                snapshotLoader: SnapshotLoader,
                                snapshotVerification: SnapshotVerification,
                                val snapshotApiRoute: EnabledSnapshotApiRoute,
                                snapshotStatusPublisher: Observer[SnapshotStatus])
    extends SnapshotComponents {

  override def close(mode: ShutdownMode.Mode): Unit = {
    snapshotLoader.close()
    snapshotBroadcaster.close()
    snapshotVerification.close()
    consensualSnapshot.close()
    if (mode == ShutdownMode.FULL_SHUTDOWN) {
      snapshotStatusPublisher.onComplete()
    }
  }
}

object SnapshotComponents {

  def apply(settings: WESettings,
            nodeOwner: PrivateKeyAccount,
            connections: ActivePeerConnections,
            notifications: ChannelObservable[SnapshotNotification],
            requests: ChannelObservable[SnapshotRequest],
            genesisRequests: ChannelObservable[GenesisSnapshotRequest],
            blockchain: BlockchainUpdater with Blockchain,
            peersIdentityService: PeersIdentityService,
            state: RocksDBWriter,
            time: Time,
            freezeApp: () => Unit)(implicit scheduler: Scheduler): Option[SnapshotComponents] = {
    settings.consensualSnapshot match {
      case DisabledSnapshot =>
        val genesisSnapshotSource = new GenesisSnapshotSource(
          initialRequests = genesisRequests,
          snapshotRequests = requests,
          peersIdentityService = peersIdentityService,
          blockchain = blockchain,
          snapshotDirectory = settings.snapshotDirectory,
          nodeOwner = nodeOwner
        )

        val apiRoute = DisabledSnapshotApiRoute(settings.api, time, nodeOwner.toAddress)

        Some(DisabledSnapshotComponents(apiRoute, genesisSnapshotSource))
      case es: EnabledSnapshot =>
        val snapshotStatusPublisher = ConcurrentSubject.publish[SnapshotStatus]
        val snapshotOpener          = new SnapshotOpener(state.fs, state.consensus, state.maxCacheSize, state.maxRollbackDepth, settings.snapshotDirectory)
        val snapshotGenesis         = new SnapshotGenesis(snapshotOpener, settings, nodeOwner, time)
        val snapshotStatusHolder    = new SnapshotStatusHolder(snapshotStatusPublisher)
        val snapshotSwap            = new SnapshotSwap(settings.snapshotDirectory, settings.dataDirectory, snapshotStatusPublisher)
        val snapshotApiRoute =
          new EnabledSnapshotApiRoute(
            snapshotStatusHolder,
            snapshotGenesis.loadGenesis(),
            snapshotSwap.asTask,
            settings.api,
            time,
            nodeOwner.toAddress,
            freezeApp
          )(scheduler)
        val snapshotBroadcaster = SnapshotBroadcaster(
          nodeOwner,
          es,
          snapshotStatusPublisher,
          connections,
          blockchain.lastBlockInfo,
          requests,
          packSnapshot(settings.snapshotDirectory, nodeOwner),
          BroadcasterTimeSpans(1.minute)
        )
        val consensualSnapshot =
          ConsensualSnapshot(es, settings.snapshotDirectory, nodeOwner, blockchain, state.storage, snapshotStatusPublisher, snapshotGenesis)
        val snapshotVerification = SnapshotVerification(
          snapshotDirectory = settings.snapshotDirectory,
          statusConcurrentSubject = snapshotStatusPublisher,
          verification = snapshotOpener.withSnapshot { snapshot =>
            new SnapshotGenesisVerifier(state, snapshot, es).verifyAsTask(scheduler) >>
              new AsyncSnapshotVerifier(state, snapshot)(scheduler).verify()
          }
        )
        val snapshotLoader = SnapshotLoader(
          nodeOwner = nodeOwner,
          snapshotDirectory = settings.snapshotDirectory,
          settings = es,
          connections = connections,
          blockchain = blockchain,
          notifications = notifications,
          statusObserver = snapshotStatusPublisher,
          unpackSnapshot = unpackSnapshot(settings.snapshotDirectory),
          timeSpans = LoaderTimeSpans(10.seconds, 10.seconds, 30.seconds)
        )
        Some(
          new EnabledSnapshotComponents(consensualSnapshot,
                                        snapshotBroadcaster,
                                        snapshotLoader,
                                        snapshotVerification,
                                        snapshotApiRoute,
                                        snapshotStatusPublisher))
    }
  }
}

class SnapshotOpener(fs: FunctionalitySettings, consensus: ConsensusSettings, maxCacheSize: Int, maxRollbackDepth: Int, snapshotDir: String) {

  def withSnapshot[A](task: RocksDBWriter => Task[A]): Task[A] = {
    withStorage(RocksDBStorage.openDB(snapshotDir))(task)
  }

  def withSnapshotReadOnly[A](task: RocksDBWriter => Task[A]): Task[A] = {
    withStorage(RocksDBStorage.openDB(path = snapshotDir, params = DefaultReadOnlyParams))(task)
  }

  private def withStorage[A](storage: => RocksDBStorage)(task: RocksDBWriter => Task[A]): Task[A] = {
    Task
      .eval(storage)
      .bracket { snapshotStorage =>
        Task.defer {
          val snapshot = new RocksDBWriter(snapshotStorage, fs, consensus, maxCacheSize, maxRollbackDepth)
          task(snapshot)
        }
      } { snapshotStorage =>
        Task(snapshotStorage.close())
      }
  }
}
