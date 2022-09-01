package com.wavesenterprise.database.snapshot

import com.wavesenterprise.database.rocksdb.RocksDBWriter
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.state.diffs.assertDiffAndState
import org.scalatest.propspec.AnyPropSpec

class StateSnapshotTest extends AnyPropSpec with BaseSnapshotTest {

  import StateSnapshotTest._

  property("check snapshot state correctness") {
    forAll(preconditionsGen(UpdatesBlocksCount)) { preconditions =>
      assertDiffAndState(preconditions.blocks, TestBlock.create(Seq.empty), fs) {
        case (_, state) =>
          val storage = state.asInstanceOf[RocksDBWriter].storage

          withDirectory("snapshot-test") { snapshotDir =>
            storage.takeSnapshot(snapshotDir.toString) shouldBe 'right
            verifySnapshot(snapshotDir, preconditions, state)
          }
      }
    }
  }
}

object StateSnapshotTest {
  private val UpdatesBlocksCount = 10
}
