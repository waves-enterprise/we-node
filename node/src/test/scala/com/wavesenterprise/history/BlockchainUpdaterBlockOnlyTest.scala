package com.wavesenterprise.history

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.transaction._
import com.wavesenterprise.transaction.transfer._
import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class BlockchainUpdaterBlockOnlyTest
    extends AnyPropSpec
    with ScalaCheckPropertyChecks
    with DomainScenarioDrivenPropertyCheck
    with Matchers
    with TransactionGen {

  def preconditionsAndPayments(paymentsAmt: Int): Gen[(GenesisTransaction, Seq[TransferTransactionV2])] =
    for {
      master    <- accountGen
      recipient <- accountGen
      ts        <- ntpTimestampGen
      genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      payments <- Gen.listOfN(paymentsAmt, westTransferGeneratorP(master, recipient.toAddress))
    } yield (genesis, payments)

  property("can apply valid blocks") {
    assume(BlockchainFeature.implemented.contains(BlockchainFeature.SmartAccounts.id))
    scenario(preconditionsAndPayments(1)) {
      case (domain, (genesis, payments)) =>
        val blocks = chainBlocks(Seq(Seq(genesis), Seq(payments.head)))
        all(blocks.map(block => domain.blockchainUpdater.processBlock(block, ConsensusPostAction.NoAction))) shouldBe 'right
    }
  }

  property("can apply, rollback and reprocess valid blocks") {
    assume(BlockchainFeature.implemented.contains(BlockchainFeature.SmartAccounts.id))
    scenario(preconditionsAndPayments(2)) {
      case (domain, (genesis, payments)) =>
        val blocks = chainBlocks(Seq(Seq(genesis), Seq(payments(0)), Seq(payments(1))))
        domain.blockchainUpdater.processBlock(blocks.head, ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.height shouldBe 1
        domain.blockchainUpdater.processBlock(blocks(1), ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.height shouldBe 2
        domain.blockchainUpdater.removeAfter(blocks.head.uniqueId) shouldBe 'right
        domain.blockchainUpdater.height shouldBe 1
        domain.blockchainUpdater.processBlock(blocks(1), ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.processBlock(blocks(2), ConsensusPostAction.NoAction) shouldBe 'right
    }
  }

  property("can't apply block with invalid signature") {
    assume(BlockchainFeature.implemented.contains(BlockchainFeature.SmartAccounts.id))
    scenario(preconditionsAndPayments(1)) {
      case (domain, (genesis, payment)) =>
        val blocks = chainBlocks(Seq(Seq(genesis), payment))
        domain.blockchainUpdater.processBlock(blocks.head, ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.processBlock(spoilSignature(blocks.last), ConsensusPostAction.NoAction) should produce("InvalidSignature")
    }
  }

  property("can't apply block with invalid signature after rollback") {
    assume(BlockchainFeature.implemented.contains(BlockchainFeature.SmartAccounts.id))
    scenario(preconditionsAndPayments(1)) {
      case (domain, (genesis, payment)) =>
        val blocks = chainBlocks(Seq(Seq(genesis), payment))
        domain.blockchainUpdater.processBlock(blocks.head, ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.processBlock(blocks(1), ConsensusPostAction.NoAction) shouldBe 'right
        domain.blockchainUpdater.removeAfter(blocks.head.uniqueId) shouldBe 'right
        domain.blockchainUpdater.processBlock(spoilSignature(blocks(1)), ConsensusPostAction.NoAction) should produce("InvalidSignature")
    }
  }

  property("can process 11 blocks and then rollback to genesis") {
    assume(BlockchainFeature.implemented.contains(BlockchainFeature.SmartAccounts.id))
    scenario(preconditionsAndPayments(10)) {
      case (domain, (genesis, payments)) =>
        val blocks = chainBlocks(Seq(genesis) +: payments.map(Seq(_)))
        blocks.foreach { b =>
          domain.blockchainUpdater.processBlock(b, ConsensusPostAction.NoAction) shouldBe 'right
        }
        domain.blockchainUpdater.removeAfter(blocks.head.uniqueId) shouldBe 'right
    }
  }
}
