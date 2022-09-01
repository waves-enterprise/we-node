package com.wavesenterprise.state.reader

import com.wavesenterprise.consensus.GeneratingBalanceProvider
import com.wavesenterprise.features.BlockchainFeature._
import com.wavesenterprise.state.LeaseBalance
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import com.wavesenterprise.lagonaki.mocks.TestBlock.{create => block}
import com.wavesenterprise.settings.TestFunctionalitySettings.Enabled
import com.wavesenterprise.transaction.GenesisTransaction
import com.wavesenterprise.transaction.lease.LeaseTransactionV2
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class StateReaderEffectiveBalancePropertyTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {
  property("No-interactions genesis account's effectiveBalance doesn't depend on depths") {
    val setup: Gen[(GenesisTransaction, Int, Int, Int)] = for {
      master <- accountGen
      ts     <- positiveIntGen
      genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      emptyBlocksAmt <- Gen.choose(1, 10)
      atHeight       <- Gen.choose(1, 20)
      confirmations  <- Gen.choose(1, 20)
    } yield (genesis, emptyBlocksAmt, atHeight, confirmations)

    forAll(setup) {
      case (genesis: GenesisTransaction, emptyBlocksAmt, atHeight, confirmations) =>
        val genesisBlock = block(Seq(genesis))
        val nextBlocks   = List.fill(emptyBlocksAmt - 1)(block(Seq.empty))
        assertDiffAndState(genesisBlock +: nextBlocks, block(Seq.empty)) { (_, newState) =>
          newState.effectiveBalance(genesis.recipient, atHeight, confirmations) shouldBe genesis.amount
        }
    }
  }

  property("Negative generating balance case") {
    val fs  = Enabled.copy(preActivatedFeatures = Map(SmartAccounts.id -> 0, SmartAccountTrading.id -> 0))
    val Fee = 100000
    val setup = for {
      master <- accountGen
      ts     <- positiveLongGen
      genesis = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      leaser <- accountGen
      xfer1  <- transferGeneratorPV2(ts + 1, master, leaser.toAddress, ENOUGH_AMT / 3)
      lease1 = LeaseTransactionV2.signed(leaser, None, leaser, master.toAddress, xfer1.amount - Fee, Fee, ts + 2).explicitGet()
      xfer2 <- transferGeneratorPV2(ts + 3, master, leaser.toAddress, ENOUGH_AMT / 3)
      lease2 = LeaseTransactionV2.signed(leaser, None, leaser, master.toAddress, xfer2.amount - Fee, Fee, ts + 4).explicitGet()
    } yield (leaser, genesis, xfer1, lease1, xfer2, lease2)

    forAll(setup) {
      case (leaser, genesis, xfer1, lease1, xfer2, lease2) =>
        assertDiffAndState(Seq(block(Seq(genesis)), block(Seq(xfer1, lease1))), block(Seq(xfer2, lease2)), fs) { (_, state) =>
          val portfolio       = state.westPortfolio(lease1.sender.toAddress)
          val expectedBalance = xfer1.amount + xfer2.amount - 2 * Fee
          portfolio.balance shouldBe expectedBalance
          GeneratingBalanceProvider.balance(state, fs, state.height, leaser.toAddress) shouldBe 0
          portfolio.lease shouldBe LeaseBalance(0, expectedBalance)
          portfolio.effectiveBalance shouldBe 0
        }
    }
  }
}
