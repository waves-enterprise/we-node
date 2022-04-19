package com.wavesenterprise.database.snapshot

import com.wavesenterprise.TestSchedulers
import com.wavesenterprise.database.rocksdb.RocksDBWriter
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.ConsensusSettings
import com.wavesenterprise.state.ContractBlockchain.ContractReadingContext
import com.wavesenterprise.state.diffs.assertDiffAndState
import com.wavesenterprise.state.{ByteStr, DataEntry, StringDataEntry}
import monix.execution.Scheduler
import org.scalatest.{Matchers, PropSpec}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._

class AsyncSnapshotVerifierTest extends PropSpec with ScalaCheckPropertyChecks with Matchers with StatePreconditionsGen {

  import AsyncSnapshotVerifierTest._

  implicit val scheduler: Scheduler = TestSchedulers.consensualSnapshotScheduler

  property("check snapshot verifier correctness") {
    forAll(preconditionsGen(UpdatesBlocksCount)) { preconditions =>
      assertDiffAndState(preconditions.blocks, TestBlock.create(Seq.empty), fs) {
        case (_, state) =>
          val writer   = state.asInstanceOf[RocksDBWriter]
          val storage  = writer.storage
          val snapshot = new RocksDBWriter(storage, fs, ConsensusSettings.PoSSettings, 100000, 2000)

          val verifier = new AsyncSnapshotVerifier(writer, snapshot)
          verifier.verify().runSyncUnsafe(10.seconds)
      }
    }
  }

  property("test negative case") {
    forAll(preconditionsGen(UpdatesBlocksCount)) { preconditions =>
      assertDiffAndState(preconditions.blocks, TestBlock.create(Seq.empty), fs) {
        case (_, state) =>
          val writer  = state.asInstanceOf[RocksDBWriter]
          val storage = writer.storage
          val snapshot = new RocksDBWriter(storage, fs, ConsensusSettings.PoSSettings, 100000, 2000) {
            override def contractData(contractId: ByteStr, key: String, readingContext: ContractReadingContext): Option[DataEntry[_]] =
              Some(StringDataEntry("key", "wrong_value"))
          }

          val verifier = new AsyncSnapshotVerifier(writer, snapshot)

          val caught = intercept[VerificationException] {
            verifier.verify().runSyncUnsafe(10.seconds)
          }
          caught.getMessage should startWith("Contract data are not equal")
      }
    }
  }
}

object AsyncSnapshotVerifierTest {
  private val UpdatesBlocksCount = 20
}
