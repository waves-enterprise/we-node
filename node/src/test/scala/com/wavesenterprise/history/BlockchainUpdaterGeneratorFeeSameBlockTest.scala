package com.wavesenterprise.history

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.consensus.ConsensusPostAction
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.transaction.GenesisTransaction
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.scalacheck.Gen
import org.scalatest._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class BlockchainUpdaterGeneratorFeeSameBlockTest
    extends AnyPropSpec
    with ScalaCheckPropertyChecks
    with DomainScenarioDrivenPropertyCheck
    with Matchers
    with TransactionGen {

  type Setup = (GenesisTransaction, TransferTransactionV2, TransferTransactionV2)

  val preconditionsAndPayments: Gen[Setup] = for {
    sender    <- accountGen
    recipient <- accountGen
    fee       <- smallFeeGen
    ts        <- ntpTimestampGen
    genesis: GenesisTransaction = GenesisTransaction.create(sender.toAddress, ENOUGH_AMT, ts - 5000).explicitGet()
    payment <- westTransferGeneratorP(sender, recipient.toAddress)
    generatorPaymentOnFee = createWestTransfer(defaultSigner, recipient.toAddress, payment.fee, fee, ts + 1).explicitGet()
  } yield (genesis, payment, generatorPaymentOnFee)

  property("block generator can spend fee after transaction before applyMinerFeeWithTransactionAfter") {
    assume(BlockchainFeature.implemented.contains(BlockchainFeature.SmartAccounts.id))
    scenario(preconditionsAndPayments, DefaultWESettings) {
      case (domain, (genesis, somePayment, generatorPaymentOnFee)) =>
        val blocks = chainBlocks(Seq(Seq(genesis), Seq(generatorPaymentOnFee, somePayment)))
        all(blocks.map(block => domain.blockchainUpdater.processBlock(block, ConsensusPostAction.NoAction))) shouldBe 'right
    }
  }

  property("block generator can't spend fee after transaction after applyMinerFeeWithTransactionAfter") {
    scenario(preconditionsAndPayments, MicroblocksActivatedAt0WESettings) {
      case (domain, (genesis, somePayment, generatorPaymentOnFee)) =>
        val blocks = chainBlocks(Seq(Seq(genesis), Seq(generatorPaymentOnFee, somePayment)))
        blocks.init.foreach(block => domain.blockchainUpdater.processBlock(block, ConsensusPostAction.NoAction).explicitGet())
        domain.blockchainUpdater.processBlock(blocks.last, ConsensusPostAction.NoAction) should produce("unavailable funds")
    }
  }
}
