package com.wavesenterprise.database.snapshot

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.acl.PermissionValidator
import com.wavesenterprise.block.{Block, GenesisData}
import com.wavesenterprise.consensus.{CftLikeConsensusBlockData, ConsensusBlockData, PoALikeConsensusBlockData}
import com.wavesenterprise.mining.MiningConstraint
import com.wavesenterprise.settings.{
  ConsensusType,
  GenesisSettingsVersion,
  PlainGenesisSettings,
  SnapshotBasedGenesisSettings,
  WESettings,
  WestAmount
}
import com.wavesenterprise.state.diffs.BlockDiffer
import com.wavesenterprise.state.diffs.CommonValidation.MaxTimePrevBlockOverTransactionDiff
import com.wavesenterprise.transaction.ValidationError
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.utils.{ScorexLogging, Time}
import monix.eval.Task
import org.rocksdb.RocksDBException

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

class SnapshotGenesis(snapshotOpener: SnapshotOpener, weSettings: WESettings, miner: PrivateKeyAccount, time: Time) extends ScorexLogging {

  import SnapshotGenesis._

  private[this] val senderRoleEnabled: Boolean = weSettings.blockchain.custom.genesis.senderRoleEnabled

  def build(consensusType: ConsensusType): Task[Either[GenericError, Block]] = Task {
    buildGenesis(consensusType, miner, time.getTimestamp(), senderRoleEnabled)
  }

  def append(genesis: Block): Task[Either[ValidationError, Unit]] = {
    snapshotOpener.withSnapshot { snapshot =>
      Task {
        for {
          blockDiff <- BlockDiffer.fromBlock(
            weSettings.blockchain,
            weSettings.consensualSnapshot,
            snapshot,
            PermissionValidator(senderRoleEnabled),
            None,
            genesis,
            MiningConstraint.Unlimited,
            MaxTimePrevBlockOverTransactionDiff,
            alreadyVerified = true
          )
          (diff, fees, _) = blockDiff
        } yield snapshot.append(diff, fees, genesis)
      }
    }
  }

  def loadGenesis(): Task[Option[Block]] = {
    snapshotOpener
      .withSnapshotReadOnly { snapshot =>
        Task(snapshot.lastBlock)
      }
      .onErrorRecoverWith {
        case e: RocksDBException =>
          log.error("Unable to retrieve snapshot genesis block", e)
          Task(None)
      }
  }
}

object SnapshotGenesis {

  private val SettingsVersion = GenesisSettingsVersion.ModernVersion

  private def selectConsensusBlockData(consensusType: ConsensusType): ConsensusBlockData = {
    consensusType match {
      case ConsensusType.PoA =>
        PoALikeConsensusBlockData(overallSkippedRounds = 0)
      case ConsensusType.CFT =>
        CftLikeConsensusBlockData(votes = Seq.empty, overallSkippedRounds = 0)
      case ConsensusType.PoS =>
        throw new IllegalArgumentException("PoS consensus type is not allowed")
    }
  }

  def buildGenesis(consensusType: ConsensusType,
                   miner: PrivateKeyAccount,
                   timestamp: Long,
                   senderRoleEnabled: Boolean): Either[GenericError, Block] = {
    val consensusBlockData = selectConsensusBlockData(consensusType)
    Block.buildAndSign(
      Block.selectGenesisBlockVersion(SettingsVersion),
      timestamp,
      Block.genesisReference(),
      consensusBlockData,
      Seq.empty,
      miner,
      Set.empty,
      Some(GenesisData(senderRoleEnabled = senderRoleEnabled))
    )
  }

  def mapToSettings(genesis: Block): PlainGenesisSettings = {
    PlainGenesisSettings(
      blockTimestamp = genesis.timestamp,
      initialBalance = WestAmount(0),
      genesisPublicKeyBase58 = genesis.signerData.generator.publicKeyBase58,
      signature = Some(genesis.signerData.signature),
      transactions = Seq.empty,
      networkParticipants = Seq.empty,
      initialBaseTarget = 0,
      averageBlockDelay = FiniteDuration(0, TimeUnit.SECONDS),
      version = SettingsVersion,
      senderRoleEnabled = genesis.genesisDataOpt.exists(_.senderRoleEnabled)
    )
  }

  def mapToSnapshotBasedSettings(genesis: Block): SnapshotBasedGenesisSettings = {
    SnapshotBasedGenesisSettings(
      blockTimestamp = genesis.timestamp,
      genesisPublicKeyBase58 = genesis.signerData.generator.publicKeyBase58,
      signature = Some(genesis.signerData.signature),
      senderRoleEnabled = genesis.genesisDataOpt.exists(_.senderRoleEnabled)
    )
  }
}
