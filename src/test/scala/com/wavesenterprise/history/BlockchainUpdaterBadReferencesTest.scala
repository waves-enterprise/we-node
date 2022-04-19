package com.wavesenterprise.history

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.block.TxMicroBlock
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.transaction.GenesisTransaction
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class BlockchainUpdaterBadReferencesTest
    extends PropSpec
    with ScalaCheckPropertyChecks
    with DomainScenarioDrivenPropertyCheck
    with Matchers
    with TransactionGen {

  val preconditionsAndPayments: Gen[(GenesisTransaction, TransferTransactionV2, TransferTransactionV2, TransferTransactionV2)] = for {
    master    <- accountGen
    recipient <- accountGen
    ts        <- ntpTimestampGen
    genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts - 5000).explicitGet()
    payment  <- westTransferGeneratorP(master, recipient.toAddress)
    payment2 <- westTransferGeneratorP(master, recipient.toAddress)
    payment3 <- westTransferGeneratorP(master, recipient.toAddress)
  } yield (genesis, payment, payment2, payment3)

  property("micro-block: invalid total signature") {
    scenario(preconditionsAndPayments, MicroblocksActivatedAt0WESettings) {
      case (domain, (genesis, payment, payment2, payment3)) =>
        val block0                 = buildBlockOfTxs(randomSig, Seq(genesis))
        val (block1, microBlocks1) = chainBaseAndMicro(block0.uniqueId, Seq(payment), Seq(payment2, payment3).map(Seq(_)))
        val goodMicro              = microBlocks1.head
        val badMicro = TxMicroBlock
          .buildAndSign(defaultSigner, microBlocks1(1).timestamp, microBlocks1(1).transactionData, microBlocks1(1).prevLiquidBlockSig, randomSig)
          .explicitGet()

        domain.blockchainUpdater.processBlock(block0, ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.processBlock(block1, ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.processMicroBlock(goodMicro) shouldBe 'right
        domain.blockchainUpdater.processMicroBlock(badMicro) should produce("Invalid liquid block total signature")
    }
  }

  property("micro-block: referenced micro-block doesn't exist") {
    scenario(preconditionsAndPayments, MicroblocksActivatedAt0WESettings) {
      case (domain, (genesis, payment, payment2, payment3)) =>
        val block0                 = buildBlockOfTxs(randomSig, Seq(genesis))
        val (block1, microBlocks1) = chainBaseAndMicro(block0.uniqueId, Seq(payment), Seq(payment2, payment3).map(Seq(_)))
        val goodMicro              = microBlocks1.head
        val badMicro = TxMicroBlock
          .buildAndSign(defaultSigner, microBlocks1(1).timestamp, microBlocks1(1).transactionData, randomSig, microBlocks1(1).totalLiquidBlockSig)
          .explicitGet()

        domain.blockchainUpdater.processBlock(block0, ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.processBlock(block1, ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.processMicroBlock(goodMicro) shouldBe 'right
        domain.blockchainUpdater.processMicroBlock(badMicro) should produce("doesn't reference last known micro-block")
    }
  }

  property("micro-block: first micro doesn't reference base block (references nothing)") {
    scenario(preconditionsAndPayments, MicroblocksActivatedAt0WESettings) {
      case (domain, (genesis, payment, payment2, _)) =>
        val blocks = chainBlocks(Seq(Seq(genesis), Seq(payment)))
        val badMicroRef = buildMicroBlockOfTxs(blocks.head.uniqueId, blocks(1), Seq(payment2), defaultSigner)._2
          .copy(prevLiquidBlockSig = randomSig)

        domain.blockchainUpdater.processBlock(blocks.head, ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.processBlock(blocks(1), ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.processMicroBlock(badMicroRef) should produce("doesn't reference base block")
    }
  }

  property("micro-block: referenced base block doesn't exist") {
    scenario(preconditionsAndPayments, MicroblocksActivatedAt0WESettings) {
      case (domain, (genesis, payment, payment2, _)) =>
        val block0                 = buildBlockOfTxs(randomSig, Seq(genesis))
        val (block1, microBlocks1) = chainBaseAndMicro(block0.uniqueId, Seq(payment), Seq(payment2).map(Seq(_)))

        domain.blockchainUpdater.processBlock(block0, ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.processBlock(block1, ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.removeAfter(block0.uniqueId) shouldBe 'right
        domain.blockchainUpdater.processMicroBlock(microBlocks1.head) should produce("No base block exists")
    }
  }

  property("block: second 'genesis' block") {
    scenario(preconditionsAndPayments, MicroblocksActivatedAt0WESettings) {
      case (domain, (genesis, payment, payment2, _)) =>
        val block0 = buildBlockOfTxs(randomSig, Seq(genesis, payment))
        val block1 = buildBlockOfTxs(randomSig, Seq(genesis, payment2))

        domain.blockchainUpdater.processBlock(block0, ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.processBlock(block1, ConsensusPostAction.NoAction) should produce("References incorrect or non-existing block")
    }
  }

  property("block: incorrect or non-existing block when liquid is empty") {
    scenario(preconditionsAndPayments, MicroblocksActivatedAt0WESettings) {
      case (domain, (genesis, payment, payment2, payment3)) =>
        val blocks = chainBlocks(Seq(Seq(genesis), Seq(payment), Seq(payment2)))

        domain.blockchainUpdater.processBlock(blocks.head, ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.processBlock(blocks(1), ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.removeAfter(blocks.head.uniqueId) shouldBe 'right

        val block2Reference = randomSig
        val block2          = buildBlockOfTxs(block2Reference, Seq(payment3))

        domain.blockchainUpdater.processBlock(block2, ConsensusPostAction.NoAction) should produce(
          s"Block reference '$block2Reference' doesn't exist")
    }
  }

  property("block: incorrect or non-existing block when liquid exists") {
    assume(BlockchainFeature.implemented.contains(BlockchainFeature.SmartAccounts.id))
    scenario(preconditionsAndPayments, MicroblocksActivatedAt0WESettings) {
      case (domain, (genesis, payment, payment2, payment3)) =>
        val blocks   = chainBlocks(Seq(Seq(genesis), Seq(payment), Seq(payment2)))
        val block1v2 = buildBlockOfTxs(blocks.head.uniqueId, Seq(payment3))

        domain.blockchainUpdater.processBlock(blocks.head, ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.processBlock(blocks(1), ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.processBlock(blocks(2), ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.processBlock(block1v2, ConsensusPostAction.NoAction) should produce("References incorrect or non-existing block")
    }
  }
}
