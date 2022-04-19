package com.wavesenterprise.transaction

import java.security.Permission
import java.util.concurrent.{Semaphore, TimeUnit}

import com.wavesenterprise.block.Block
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.db.WithDomain
import com.wavesenterprise.features.BlockchainFeatureStatus
import com.wavesenterprise.features.FeatureProvider._
import com.wavesenterprise.history
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.state._
import com.wavesenterprise.state.diffs.produce
import org.scalatest.words.ShouldVerb
import org.scalatest.{FreeSpec, Matchers}

class BlockchainUpdaterTest extends FreeSpec with Matchers with HistoryTest with ShouldVerb with WithDomain {

  private val ApprovalPeriod = 100

  private val WESettings = history.DefaultWESettings.copy(
    blockchain = history.DefaultWESettings.blockchain
      .copy(
        custom = history.DefaultWESettings.blockchain.custom.copy(
          functionality = history.DefaultWESettings.blockchain.custom.functionality.copy(
            featureCheckBlocksPeriod = ApprovalPeriod,
            blocksForFeatureActivation = (ApprovalPeriod * 0.9).toInt,
            preActivatedFeatures = Map.empty
          ),
        )
      ),
    features = history.DefaultWESettings.features.copy(autoShutdownOnUnsupportedFeature = true)
  )

  private val WESettingsWithDoubling = WESettings.copy(
    blockchain = WESettings.blockchain.copy(
      custom = history.DefaultWESettings.blockchain.custom.copy(
        functionality = WESettings.blockchain.custom.functionality.copy(
          preActivatedFeatures = Map.empty
        )
      )
    )
  )

  def appendBlock(block: Block, blockchainUpdater: BlockchainUpdater): Unit = {
    blockchainUpdater.processBlock(block, ConsensusPostAction.NoAction)
  }

  "features approved and accepted as height grows" in withDomain(WESettings) { domain =>
    val b = domain.blockchainUpdater

    b.processBlock(genesisBlock, ConsensusPostAction.NoAction)

    b.featureStatus(2, 1) shouldBe BlockchainFeatureStatus.Undefined
    b.featureStatus(3, 1) shouldBe BlockchainFeatureStatus.Undefined

    (2 to ApprovalPeriod).foreach { _ =>
      b.processBlock(getNextTestBlockWithVotes(b, Set(2)), ConsensusPostAction.NoAction)
    }

    b.height shouldBe ApprovalPeriod
    b.featureStatus(2, ApprovalPeriod) shouldBe BlockchainFeatureStatus.Approved
    b.featureStatus(3, ApprovalPeriod) shouldBe BlockchainFeatureStatus.Undefined

    (1 to ApprovalPeriod).foreach { _ =>
      b.processBlock(getNextTestBlockWithVotes(b, Set()), ConsensusPostAction.NoAction)
    }

    b.height shouldBe 2 * ApprovalPeriod
    b.featureStatus(2, 2 * ApprovalPeriod) shouldBe BlockchainFeatureStatus.Activated
    b.featureStatus(3, 2 * ApprovalPeriod) shouldBe BlockchainFeatureStatus.Undefined
  }

  "features rollback with block rollback" in withDomain(WESettings) { domain =>
    val b = domain.blockchainUpdater
    b.processBlock(genesisBlock, ConsensusPostAction.NoAction)

    b.featureStatus(2, 1) shouldBe BlockchainFeatureStatus.Undefined
    b.featureStatus(3, 1) shouldBe BlockchainFeatureStatus.Undefined

    (1 until ApprovalPeriod).foreach { _ =>
      b.processBlock(getNextTestBlockWithVotes(b, Set(2)), ConsensusPostAction.NoAction).explicitGet()
    }

    b.height shouldBe ApprovalPeriod
    b.featureStatus(2, ApprovalPeriod) shouldBe BlockchainFeatureStatus.Approved
    b.featureStatus(3, ApprovalPeriod) shouldBe BlockchainFeatureStatus.Undefined

    b.removeAfter(b.lastBlockIds(2).last).explicitGet()

    b.height shouldBe ApprovalPeriod - 1
    b.featureStatus(2, ApprovalPeriod - 1) shouldBe BlockchainFeatureStatus.Undefined
    b.featureStatus(3, ApprovalPeriod - 1) shouldBe BlockchainFeatureStatus.Undefined

    (1 to ApprovalPeriod + 1).foreach { _ =>
      b.processBlock(getNextTestBlockWithVotes(b, Set(3)), ConsensusPostAction.NoAction).explicitGet()
    }

    b.height shouldBe 2 * ApprovalPeriod
    b.featureStatus(2, 2 * ApprovalPeriod) shouldBe BlockchainFeatureStatus.Activated
    b.featureStatus(3, 2 * ApprovalPeriod) shouldBe BlockchainFeatureStatus.Approved

    b.removeAfter(b.lastBlockIds(2).last).explicitGet()

    b.height shouldBe 2 * ApprovalPeriod - 1
    b.featureStatus(2, 2 * ApprovalPeriod - 1) shouldBe BlockchainFeatureStatus.Approved
    b.featureStatus(3, 2 * ApprovalPeriod - 1) shouldBe BlockchainFeatureStatus.Undefined

    b.processBlock(getNextTestBlockWithVotes(b, Set.empty), ConsensusPostAction.NoAction).explicitGet()

    b.height shouldBe 2 * ApprovalPeriod
    b.featureStatus(2, 2 * ApprovalPeriod) shouldBe BlockchainFeatureStatus.Activated
    b.featureStatus(3, 2 * ApprovalPeriod) shouldBe BlockchainFeatureStatus.Approved

    b.removeAfter(b.lastBlockIds(2).last).explicitGet()

    b.height shouldBe 2 * ApprovalPeriod - 1
    b.featureStatus(2, 2 * ApprovalPeriod - 1) shouldBe BlockchainFeatureStatus.Approved
    b.featureStatus(3, 2 * ApprovalPeriod - 1) shouldBe BlockchainFeatureStatus.Undefined

    b.removeAfter(b.lastBlockIds(ApprovalPeriod + 1).last).explicitGet()

    b.height shouldBe ApprovalPeriod - 1
    b.featureStatus(2, ApprovalPeriod - 1) shouldBe BlockchainFeatureStatus.Undefined
    b.featureStatus(3, ApprovalPeriod - 1) shouldBe BlockchainFeatureStatus.Undefined
  }

  "feature activation height is not overriden with further periods" in withDomain(WESettings) { domain =>
    val b = domain.blockchainUpdater

    b.processBlock(genesisBlock, ConsensusPostAction.NoAction)

    b.featureStatus(2, 1) shouldBe BlockchainFeatureStatus.Undefined

    b.featureActivationHeight(2) shouldBe None

    (1 until ApprovalPeriod).foreach { _ =>
      b.processBlock(getNextTestBlockWithVotes(b, Set(2)), ConsensusPostAction.NoAction).explicitGet()
    }

    b.featureActivationHeight(2) shouldBe Some(ApprovalPeriod * 2)

    (1 to ApprovalPeriod).foreach { _ =>
      b.processBlock(getNextTestBlockWithVotes(b, Set(2)), ConsensusPostAction.NoAction).explicitGet()
    }

    b.featureActivationHeight(2) shouldBe Some(ApprovalPeriod * 2)
  }

  "feature activated only by 90% of blocks" in withDomain(WESettings) { domain =>
    val b = domain.blockchainUpdater

    b.processBlock(genesisBlock, ConsensusPostAction.NoAction)

    b.featureStatus(2, 1) shouldBe BlockchainFeatureStatus.Undefined

    (1 until ApprovalPeriod).foreach { i =>
      b.processBlock(getNextTestBlockWithVotes(b, if (i % 2 == 0) Set(2) else Set()), ConsensusPostAction.NoAction)
        .explicitGet()
    }
    b.featureStatus(2, ApprovalPeriod) shouldBe BlockchainFeatureStatus.Undefined

    (1 to ApprovalPeriod).foreach { i =>
      b.processBlock(getNextTestBlockWithVotes(b, if (i % 10 == 0) Set() else Set(2)), ConsensusPostAction.NoAction)
        .explicitGet()
    }
    b.featureStatus(2, ApprovalPeriod * 2) shouldBe BlockchainFeatureStatus.Approved

    (1 to ApprovalPeriod).foreach { i =>
      b.processBlock(getNextTestBlock(b), ConsensusPostAction.NoAction).explicitGet()
    }
    b.featureStatus(2, ApprovalPeriod * 3) shouldBe BlockchainFeatureStatus.Activated
  }

  "features votes resets when voting window changes" in withDomain(WESettings) { domain =>
    val b = domain.blockchainUpdater

    b.processBlock(genesisBlock, ConsensusPostAction.NoAction)

    b.featureVotes(b.height) shouldBe Map.empty

    b.featureStatus(2, b.height) shouldBe BlockchainFeatureStatus.Undefined

    (1 until ApprovalPeriod).foreach { i =>
      b.processBlock(getNextTestBlockWithVotes(b, Set(2)), ConsensusPostAction.NoAction)
      b.featureVotes(b.height) shouldBe Map(2.toShort -> i)
    }

    b.featureStatus(2, b.height) shouldBe BlockchainFeatureStatus.Approved

    b.processBlock(getNextTestBlockWithVotes(b, Set(2)), ConsensusPostAction.NoAction)
    b.featureVotes(b.height) shouldBe Map(2.toShort -> 1)

    b.featureStatus(2, b.height) shouldBe BlockchainFeatureStatus.Approved
  }

  "block processing should fail if unimplemented feature was activated on blockchain when autoShutdownOnUnsupportedFeature = yes and exit with code 38" in withDomain(
    WESettings) { domain =>
    val b      = domain.blockchainUpdater
    val signal = new Semaphore(1)
    signal.acquire()

    System.setSecurityManager(new SecurityManager {
      override def checkPermission(perm: Permission): Unit = {}

      override def checkPermission(perm: Permission, context: Object): Unit = {}

      override def checkExit(status: Int): Unit = signal.synchronized {
        super.checkExit(status)
        if (status == 38)
          signal.release()
        throw new SecurityException("System exit is not allowed")
      }
    })

    b.processBlock(genesisBlock, ConsensusPostAction.NoAction)

    (1 to ApprovalPeriod * 2).foreach { i =>
      b.processBlock(getNextTestBlockWithVotes(b, Set(-1)), ConsensusPostAction.NoAction).explicitGet()
    }

    b.processBlock(getNextTestBlockWithVotes(b, Set(-1)), ConsensusPostAction.NoAction) should produce("ACTIVATED ON BLOCKCHAIN")

    signal.tryAcquire(10, TimeUnit.SECONDS)

    System.setSecurityManager(null)
  }

  "sunny day test when known feature activated" in withDomain(WESettings) { domain =>
    val b = domain.blockchainUpdater
    b.processBlock(genesisBlock, ConsensusPostAction.NoAction)

    (1 until ApprovalPeriod * 2 - 1).foreach { _ =>
      b.processBlock(getNextTestBlockWithVotes(b, Set(2)), ConsensusPostAction.NoAction).explicitGet()
    }

    b.featureStatus(2, b.height) should be(BlockchainFeatureStatus.Approved)
    b.processBlock(getNextTestBlockWithVotes(b, Set(2)), ConsensusPostAction.NoAction).explicitGet()
    b.featureStatus(2, b.height) should be(BlockchainFeatureStatus.Activated)
  }

  "empty blocks should not disable activation" in withDomain(WESettings) { domain =>
    val b = domain.blockchainUpdater

    b.processBlock(genesisBlock, ConsensusPostAction.NoAction)
    // Start from 1 because of the genesis block
    (1 until ApprovalPeriod - 1).foreach { _ =>
      b.processBlock(getNextTestBlockWithVotes(b, Set(2)), ConsensusPostAction.NoAction).explicitGet()
    }

    b.featureStatus(2, b.height) should be(BlockchainFeatureStatus.Undefined)
    b.processBlock(getNextTestBlockWithVotes(b, Set(2)), ConsensusPostAction.NoAction).explicitGet()
    b.featureStatus(2, b.height) should be(BlockchainFeatureStatus.Approved)

    (0 until ApprovalPeriod - 1).foreach { _ =>
      b.processBlock(getNextTestBlockWithVotes(b, Set()), ConsensusPostAction.NoAction).explicitGet()
    }

    b.featureStatus(2, b.height) should be(BlockchainFeatureStatus.Approved)
    b.processBlock(getNextTestBlockWithVotes(b, Set()), ConsensusPostAction.NoAction).explicitGet()
    b.featureStatus(2, b.height) should be(BlockchainFeatureStatus.Activated)

    (0 until ApprovalPeriod * 2).foreach { _ =>
      b.processBlock(getNextTestBlockWithVotes(b, Set()), ConsensusPostAction.NoAction).explicitGet()
    }

    (0 until ApprovalPeriod - 1).foreach { _ =>
      b.processBlock(getNextTestBlockWithVotes(b, Set(3)), ConsensusPostAction.NoAction).explicitGet()
    }
    b.featureStatus(3, b.height) should be(BlockchainFeatureStatus.Undefined)
    b.processBlock(getNextTestBlockWithVotes(b, Set(3)), ConsensusPostAction.NoAction).explicitGet()
    b.featureStatus(3, b.height) should be(BlockchainFeatureStatus.Approved)

    (0 until ApprovalPeriod - 1).foreach { _ =>
      b.processBlock(getNextTestBlockWithVotes(b, Set()), ConsensusPostAction.NoAction).explicitGet()
    }
    b.featureStatus(3, b.height) should be(BlockchainFeatureStatus.Approved)
    b.processBlock(getNextTestBlockWithVotes(b, Set()), ConsensusPostAction.NoAction).explicitGet()
    b.featureStatus(3, b.height) should be(BlockchainFeatureStatus.Activated)
  }

  "doubling of feature periods works in the middle of activation period" in withDomain(WESettingsWithDoubling) { domain =>
    val b = domain.blockchainUpdater

    b.processBlock(genesisBlock, ConsensusPostAction.NoAction)
    // Start from 1 because of the genesis block
    (1 until ApprovalPeriod * 2 - 1).foreach { _ =>
      b.processBlock(getNextTestBlockWithVotes(b, Set(2)), ConsensusPostAction.NoAction).explicitGet()
    }

    b.featureStatus(2, b.height) should be(BlockchainFeatureStatus.Approved)
    b.processBlock(getNextTestBlockWithVotes(b, Set(2)), ConsensusPostAction.NoAction).explicitGet()
    b.featureStatus(2, b.height) should be(BlockchainFeatureStatus.Activated)

    // 200 blocks passed
    (0 until ApprovalPeriod - 1).foreach { _ =>
      b.processBlock(getNextTestBlockWithVotes(b, Set(3)), ConsensusPostAction.NoAction).explicitGet()
    }
    b.featureStatus(3, b.height) should be(BlockchainFeatureStatus.Undefined)
    b.processBlock(getNextTestBlockWithVotes(b, Set(3)), ConsensusPostAction.NoAction).explicitGet()
    b.featureStatus(3, b.height) should be(BlockchainFeatureStatus.Approved)

    // 300 blocks passed, the activation period should be doubled now
    (0 until ApprovalPeriod - 1).foreach { _ =>
      b.processBlock(getNextTestBlockWithVotes(b, Set()), ConsensusPostAction.NoAction).explicitGet()
    }
    b.featureStatus(3, b.height) should be(BlockchainFeatureStatus.Approved)
    b.processBlock(getNextTestBlockWithVotes(b, Set()), ConsensusPostAction.NoAction).explicitGet()
    b.featureStatus(3, b.height) should be(BlockchainFeatureStatus.Activated)
  }
}
