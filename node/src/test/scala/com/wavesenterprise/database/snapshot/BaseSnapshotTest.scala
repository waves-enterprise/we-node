package com.wavesenterprise.database.snapshot

import java.nio.file.Path

import com.wavesenterprise.database.rocksdb.{MainRocksDBStorage, RocksDBWriter}
import com.wavesenterprise.settings.ConsensusSettings
import com.wavesenterprise.state.Blockchain
import com.wavesenterprise.utils.ResourceUtils
import org.scalatest.Suite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers

trait BaseSnapshotTest extends ScalaCheckPropertyChecks with Matchers with StatePreconditionsGen { _: Suite =>

  protected def withOpenedSnapshot[A](snapshotDir: Path)(f: RocksDBWriter => A): A = {
    ResourceUtils.withResource(MainRocksDBStorage.openDB(snapshotDir.toString)) { snapshotStorage =>
      val snapshot = new RocksDBWriter(snapshotStorage, fs, ConsensusSettings.PoSSettings, 100000, 2000)
      f(snapshot)
    }
  }

  protected def verifySnapshot(snapshotDir: Path, preconditions: Preconditions, state: Blockchain): Unit = {
    withOpenedSnapshot(snapshotDir) { snapshot =>
      val verifier = new SyncSnapshotVerifier(state, snapshot)
      verifier.verify() shouldBe 'right

      for (alias <- preconditions.aliases) {
        snapshot.resolveAlias(alias) shouldBe state.resolveAlias(alias)
      }
    }
  }
}
