package com.wavesenterprise.database.snapshot

import com.wavesenterprise.RxSetup
import com.wavesenterprise.database.rocksdb.RocksDBWriter
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.state.diffs.assertDiffAndState
import com.wavesenterprise.utils.ResourceUtils.withResource
import tools.GenHelper._

import java.nio.file.Files
import scala.collection.JavaConverters._
import org.scalatest.freespec.AnyFreeSpec

class PackedSnapshotSpec extends AnyFreeSpec with BaseSnapshotTest with RxSetup {

  import PackedSnapshotSpec._

  "should pack and unpack the same files and pass verification" in {
    val preconditions = preconditionsGen(UpdatesBlocksCount).generateSample()
    assertDiffAndState(preconditions.blocks, TestBlock.create(Seq.empty), fs) {
      case (_, state) =>
        val storage = state.asInstanceOf[RocksDBWriter].storage

        withDirectory("packed-snapshot-test") { snapshotDir =>
          val snapshotDirStr = snapshotDir.toString
          storage.takeSnapshot(snapshotDirStr) shouldBe 'right

          val packedSnapshot = test(PackedSnapshot.packSnapshot(snapshotDirStr, preconditions.senders.head))
          withDirectory("packed-snapshot-test-unpack") { unpackedDir =>
            test(PackedSnapshot.verifySnapshotZip(packedSnapshot)) shouldBe 'right
            test(PackedSnapshot.unpackSnapshot(packedSnapshot, unpackedDir))

            Files.delete(packedSnapshot)

            withResource(Files.list(snapshotDir)) { expected =>
              withResource(Files.list(unpackedDir)) { unpacked =>
                val expectedSorted = expected.iterator().asScala.toVector.map(snapshotDir.relativize).sortBy(_.toString)
                val unpackedSorted = unpacked.iterator().asScala.toVector.map(unpackedDir.relativize).sortBy(_.toString)

                expectedSorted shouldBe unpackedSorted
              }
            }
            verifySnapshot(unpackedDir, preconditions, state)
          }
        }
    }
  }
}

object PackedSnapshotSpec {
  private val UpdatesBlocksCount = 30
}
