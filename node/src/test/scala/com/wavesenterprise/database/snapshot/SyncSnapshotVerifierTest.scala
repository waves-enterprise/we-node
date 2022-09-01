package com.wavesenterprise.database.snapshot

import com.wavesenterprise.database.rocksdb.RocksDBWriter
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.ConsensusSettings
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.state.diffs.assertDiffAndState
import com.wavesenterprise.state.{ByteStr, DataEntry, StringDataEntry}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class SyncSnapshotVerifierTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with StatePreconditionsGen {

  import SyncSnapshotVerifierTest._

  property("check snapshot verifier correctness") {
    forAll(preconditionsGen(UpdatesBlocksCount)) { preconditions =>
      assertDiffAndState(preconditions.blocks, TestBlock.create(Seq.empty), fs) {
        case (_, state) =>
          val storage  = state.asInstanceOf[RocksDBWriter].storage
          val snapshot = new RocksDBWriter(storage, fs, ConsensusSettings.PoSSettings, 100000, 2000)

          val verifier = new SyncSnapshotVerifier(state, snapshot)
          verifier.verify() shouldBe 'right
      }
    }
  }

  property("test negative case") {
    forAll(preconditionsGen(UpdatesBlocksCount)) { preconditions =>
      assertDiffAndState(preconditions.blocks, TestBlock.create(Seq.empty), fs) {
        case (_, state) =>
          val storage = state.asInstanceOf[RocksDBWriter].storage
          val snapshot = new RocksDBWriter(storage, fs, ConsensusSettings.PoSSettings, 100000, 2000) {
            override def contractData(contractId: ByteStr, key: String, readingContext: ContractReadingContext): Option[DataEntry[_]] =
              Some(StringDataEntry("key", "wrong_value"))
          }

          val verifier = new SyncSnapshotVerifier(state, snapshot)
          val result   = verifier.verify()
          result shouldBe 'left
          result.left.get.message should startWith("Contract data are not equal")
      }
    }
  }
}

object SyncSnapshotVerifierTest {
  private val UpdatesBlocksCount = 20
}
