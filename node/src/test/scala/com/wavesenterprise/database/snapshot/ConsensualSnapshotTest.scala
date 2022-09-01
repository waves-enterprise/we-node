package com.wavesenterprise.database.snapshot

import java.nio.file.{Files, Path}

import com.wavesenterprise.RxSetup
import com.wavesenterprise.TestSchedulers.consensualSnapshotScheduler
import com.wavesenterprise.block.Block
import com.wavesenterprise.db.WithDomain
import com.wavesenterprise.history.{DefaultWESettings, Domain}
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.{ConsensusType, PositiveInt, WESettings}
import com.wavesenterprise.utils.ResourceUtils
import monix.reactive.Observable
import monix.reactive.subjects.ConcurrentSubject
import org.scalacheck.Gen

import scala.concurrent.duration._
import org.scalatest.propspec.AnyPropSpec

class ConsensualSnapshotTest extends AnyPropSpec with BaseSnapshotTest with WithDomain with RxSetup {

  import ConsensualSnapshotTest._

  protected def createWESettings(snapshotSettings: ConsensualSnapshotSettings): WESettings = {
    val custom = DefaultWESettings.blockchain.custom.copy(functionality = fs)
    DefaultWESettings.copy(blockchain = DefaultWESettings.blockchain.copy(custom = custom), consensualSnapshot = snapshotSettings)
  }

  protected def emptyBlocksSeqGen(count: Int, refBlock: Block): Gen[Seq[Block]] = {
    def emptyBlockGen(refBlock: Block): Gen[Block] = {
      for {
        timestamp <- ntpTimestampGen
        block = TestBlock.create(timestamp, refBlock.uniqueId, Seq.empty, TestBlock.defaultSigner)
      } yield block
    }

    def gen(previous: Seq[Block]): Gen[Seq[Block]] = {
      emptyBlockGen(previous.lastOption.getOrElse(refBlock)).flatMap { block =>
        val result = previous :+ block
        if (result.size == count) result else gen(result)
      }
    }
    gen(Vector.empty)
  }

  case class FixtureParams(consensualSnapshot: ConsensualSnapshot,
                           preconditions: Preconditions,
                           d: Domain,
                           statusObservable: Observable[SnapshotStatus])

  protected def withConsensualSnapshot(snapshotDir: Path, snapshotHeight: Int = DefaultSnapshotHeight, waitBlocks: Int = DefaultWaitBlocks)(
      block: FixtureParams => Unit): Unit = {
    val snapshotSettings = createSnapshotSettings(snapshotHeight, waitBlocks)
    val settings         = createWESettings(snapshotSettings)
    val statusPublisher  = ConcurrentSubject.publish[SnapshotStatus](consensualSnapshotScheduler)
    withDomain(settings) { d =>
      val snapshotOpener =
        new SnapshotOpener(settings.blockchain.custom.functionality, settings.blockchain.consensus, 100000, 2000, snapshotDir.toString)
      val snapshotGenesis = new SnapshotGenesis(snapshotOpener, settings, TestBlock.defaultSigner, ntpTime)
      ResourceUtils.withResource {
        ConsensualSnapshot(snapshotSettings,
                           snapshotDir.toString,
                           TestBlock.defaultSigner,
                           d.blockchainUpdater,
                           d.storage,
                           statusPublisher,
                           snapshotGenesis)(consensualSnapshotScheduler)
      } { consensualSnapshot =>
        val updatesBlocks = snapshotHeight - 3 // minus genesis, setup and empty snapshot blocks
        val preconditions = preconditionsGen(updatesBlocks).sample.get
        val blocks        = preconditions.blocks
        blocks.foreach(b => d.appendBlock(b))

        val emptyBlocks = emptyBlocksSeqGen(MinBlocksCount + waitBlocks - 1, blocks.last).sample.get
        emptyBlocks.foreach(b => d.appendBlock(b))

        block(FixtureParams(consensualSnapshot, preconditions, d, statusPublisher))
      }
    }
  }

  property("check consensual snapshot was taken successfully") {
    withDirectory("consensual-snapshot-test") { snapshotDir =>
      withConsensualSnapshot(snapshotDir, waitBlocks = 1) {
        case FixtureParams(_, preconditions, d, statusObservable) =>
          val statusEvents = loadEvents(statusObservable)
          val status       = restartUntilStatus[Verified.type](statusEvents).runSyncUnsafe(10.seconds) // wait snapshot taking result

          status shouldBe Verified
          verifySnapshot(snapshotDir, preconditions, d.blockchainUpdater)
          verifySnapshotGenesis(snapshotDir)
      }
    }
  }

  property("consensual snapshot fails if snapshot directory is not empty") {
    withDirectory("consensual-snapshot-test") { snapshotDir =>
      Files.createFile(snapshotDir.resolve("some_file"))

      withConsensualSnapshot(snapshotDir, waitBlocks = 1) {
        case FixtureParams(_, _, _, statusObservable) =>
          val statusEvents = loadEvents(statusObservable)
          val status       = restartUntilStatus[Failed](statusEvents).runSyncUnsafe(10.seconds)

          status shouldBe a[Failed]
          val failed = status.asInstanceOf[Failed]
          failed.error should startWith(s"Snapshot directory '$snapshotDir' is not empty")
      }
    }
  }

  protected def verifySnapshotGenesis(snapshotDir: Path): Unit = {
    withOpenedSnapshot(snapshotDir) { snapshot =>
      val genesisOpt = snapshot.lastBlock
      genesisOpt should be(defined)
      val genesis = genesisOpt.get
      genesis.signerData.generator shouldBe TestBlock.defaultSigner
      genesis.consensusData.asCftMaybe() shouldBe 'right
    }
  }
}

object ConsensualSnapshotTest {
  private val DefaultSnapshotHeight = 20
  private val DefaultWaitBlocks     = 10
  private val MinBlocksCount        = 2

  def createSnapshotSettings(snapshotHeight: Int = DefaultSnapshotHeight, waitBlocks: Int = DefaultWaitBlocks): EnabledSnapshot = {
    EnabledSnapshot(PositiveInt(snapshotHeight), PositiveInt(waitBlocks), Backoff(2, 2.second), ConsensusType.CFT)
  }
}
