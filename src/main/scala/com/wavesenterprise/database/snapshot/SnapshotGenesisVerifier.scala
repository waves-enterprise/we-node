package com.wavesenterprise.database.snapshot

import com.wavesenterprise.consensus.{CftLikeConsensusBlockData, ConsensusBlockData, PoALikeConsensusBlockData}
import com.wavesenterprise.database.rocksdb.RocksDBWriter
import com.wavesenterprise.settings.ConsensusType
import com.wavesenterprise.utils.ScorexLogging
import monix.eval.Task
import monix.execution.Scheduler

class SnapshotGenesisVerifier(state: RocksDBWriter, snapshot: RocksDBWriter, settings: EnabledSnapshot) extends ScorexLogging {
  def verify(): Either[VerificationError, Unit] = {
    val snapshotHeight = settings.snapshotHeight.value
    val isExpectedConsensusType: ConsensusBlockData => Boolean = {
      case CftLikeConsensusBlockData(_, _) if settings.consensusType == ConsensusType.CFT => true
      case PoALikeConsensusBlockData(_) if settings.consensusType == ConsensusType.PoA    => true
      // PoS is not supported anyway
      case _ => false
    }

    for {
      minerAtSnapshotHeight <- state
        .blockHeaderAt(snapshotHeight)
        .map(_.signerData.generatorAddress)
        .toRight(VerificationError(s"Block at snapshotHeight '$snapshotHeight' is not found in the state"))

      snapshotGenesisHeader <- snapshot
        .blockHeaderAt(1)
        .toRight(VerificationError("Genesis block is not found in the snapshot"))
      snapshotGenesisMiner = snapshotGenesisHeader.signerData.generatorAddress

      _ <- Either.cond(
        minerAtSnapshotHeight == snapshotGenesisMiner,
        (),
        VerificationError(
          s"Unexpected snapshot genesis miner '$snapshotGenesisMiner'. Was expecting '$minerAtSnapshotHeight', a miner at snapshotHeight '$snapshotHeight'")
      )

      _ <- Either.cond(
        isExpectedConsensusType(snapshotGenesisHeader.consensusData),
        (),
        VerificationError(s"Unexpected consensus type in the snapshot genesis block, expected '${settings.consensusType.value}'")
      )
    } yield ()
  }

  def verifyAsTask(scheduler: Scheduler): Task[Unit] = {
    Task
      .defer {
        verify()
          .fold(error => Task.raiseError(new VerificationException(error.message)), Task.pure)
      }
      .executeOn(scheduler)
      .logErr
      .executeAsync
  }
}
