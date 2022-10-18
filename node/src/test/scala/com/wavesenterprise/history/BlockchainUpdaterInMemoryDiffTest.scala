package com.wavesenterprise.history

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class BlockchainUpdaterInMemoryDiffTest
    extends AnyPropSpec
    with ScalaCheckPropertyChecks
    with DomainScenarioDrivenPropertyCheck
    with Matchers
    with TransactionGen {
  val preconditionsAndPayments: Gen[(GenesisTransaction, TransferTransactionV2, TransferTransactionV2)] = for {
    master    <- accountGen
    recipient <- accountGen
    ts        <- ntpTimestampGen
    genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts - 5000).explicitGet()
    payment  <- westTransferGeneratorP(master, recipient.toAddress)
    payment2 <- westTransferGeneratorP(master, recipient.toAddress)
  } yield (genesis, payment, payment2)

  property("compaction with liquid block doesn't make liquid block affect state once") {
    assume(BlockchainFeature.implemented.contains(BlockchainFeature.SmartAccounts.id))
    scenario(preconditionsAndPayments) {
      case (domain, (genesis, payment1, payment2)) =>
        val blocksWithoutCompaction = chainBlocks(
          Seq(genesis) +:
            Seq.fill(MaxTransactionsPerBlockDiff * 2 - 1)(Seq.empty[Transaction]) :+
            Seq(payment1))
        val blockTriggersCompaction = buildBlockOfTxs(blocksWithoutCompaction.last.uniqueId, Seq(payment2))

        blocksWithoutCompaction.foreach(b => domain.blockchainUpdater.processBlock(b, ConsensusPostAction.NoAction).explicitGet())
        val mastersBalanceAfterPayment1 = domain.portfolio(genesis.recipient).balance
        mastersBalanceAfterPayment1 shouldBe (ENOUGH_AMT - payment1.amount - payment1.fee)

        domain.blockchainUpdater.height shouldBe MaxTransactionsPerBlockDiff * 2 + 1

        domain.blockchainUpdater.processBlock(blockTriggersCompaction, ConsensusPostAction.NoAction).explicitGet()

        domain.blockchainUpdater.height shouldBe MaxTransactionsPerBlockDiff * 2 + 2

        val mastersBalanceAfterPayment1AndPayment2 = domain.blockchainUpdater.addressBalance(genesis.recipient)
        mastersBalanceAfterPayment1AndPayment2 shouldBe (ENOUGH_AMT - payment1.amount - payment1.fee - payment2.amount - payment2.fee)
    }
  }
  property("compaction without liquid block doesn't make liquid block affect state once") {
    assume(BlockchainFeature.implemented.contains(BlockchainFeature.SmartAccounts.id))
    scenario(preconditionsAndPayments) {
      case (domain, (genesis, payment1, payment2)) =>
        val firstBlocks             = chainBlocks(Seq(Seq(genesis)) ++ Seq.fill(MaxTransactionsPerBlockDiff * 2 - 2)(Seq.empty[Transaction]))
        val payment1Block           = buildBlockOfTxs(firstBlocks.last.uniqueId, Seq(payment1))
        val emptyBlock              = buildBlockOfTxs(payment1Block.uniqueId, Seq.empty)
        val blockTriggersCompaction = buildBlockOfTxs(payment1Block.uniqueId, Seq(payment2))

        firstBlocks.foreach(b => domain.blockchainUpdater.processBlock(b, ConsensusPostAction.NoAction).explicitGet())
        domain.blockchainUpdater.processBlock(payment1Block, ConsensusPostAction.NoAction).explicitGet()
        domain.blockchainUpdater.processBlock(emptyBlock, ConsensusPostAction.NoAction).explicitGet()
        val mastersBalanceAfterPayment1 = domain.blockchainUpdater.addressBalance(genesis.recipient)
        mastersBalanceAfterPayment1 shouldBe (ENOUGH_AMT - payment1.amount - payment1.fee)

        // discard liquid block
        domain.blockchainUpdater.removeAfter(payment1Block.uniqueId)
        domain.blockchainUpdater.processBlock(blockTriggersCompaction, ConsensusPostAction.NoAction).explicitGet()

        domain.blockchainUpdater.height shouldBe MaxTransactionsPerBlockDiff * 2 + 1

        val mastersBalanceAfterPayment1AndPayment2 = domain.blockchainUpdater.addressBalance(genesis.recipient)
        mastersBalanceAfterPayment1AndPayment2 shouldBe (ENOUGH_AMT - payment1.amount - payment1.fee - payment2.amount - payment2.fee)
    }
  }
}
