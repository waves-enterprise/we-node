package com.wavesenterprise.transaction

import com.wavesenterprise.consensus.ConsensusPostActionDiff
import com.wavesenterprise.history._
import com.wavesenterprise.certs.CertChainStore
import com.wavesenterprise.state.appender.BaseAppender.BlockType.Liquid
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.state.{Diff, NgState}
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class NgStateTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  def preconditionsAndPayments(amt: Int): Gen[(GenesisTransaction, Seq[TransferTransactionV2])] =
    for {
      master    <- accountGen
      recipient <- accountGen
      ts        <- positiveIntGen
      genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      payments <- Gen.listOfN(amt, westTransferGeneratorP(master, recipient.toAddress))
    } yield (genesis, payments)

  property("can forge correctly signed blocks") {
    forAll(preconditionsAndPayments(10)) {
      case (genesis, payments) =>
        val (block, microBlocks) = chainBaseAndMicro(randomSig, Seq(genesis), payments.map(t => Seq(t)))

        val ng = new NgState(block, Liquid, Diff.empty, 0L, 0L, Set.empty, ConsensusPostActionDiff.empty)
        microBlocks.foreach(m => ng.append(m, Diff.empty, 0L, 0L, CertChainStore.empty, Set.empty))

        ng.totalDiffOf(microBlocks.last.totalLiquidBlockSig)
        microBlocks.foreach { m =>
          ng.totalDiffOf(m.totalLiquidBlockSig).get match {
            case (forged, _, _, _, _) => Signed.validate(forged) shouldBe 'right
            case _                    => ???
          }
        }
        Seq(microBlocks(4)).map(x => ng.totalDiffOf(x.totalLiquidBlockSig))
    }
  }

  property("can resolve best liquid block") {
    forAll(preconditionsAndPayments(5)) {
      case (genesis, payments) =>
        val (block, microBlocks) = chainBaseAndMicro(randomSig, Seq(genesis), payments.map(t => Seq(t)))

        val ng = new NgState(block, Liquid, Diff.empty, 0L, 0L, Set.empty, ConsensusPostActionDiff.empty)
        microBlocks.foreach(m => ng.append(m, Diff.empty, 0L, 0L, CertChainStore.empty, Set.empty))

        ng.bestLiquidBlock.uniqueId shouldBe microBlocks.last.totalLiquidBlockSig

        new NgState(block, Liquid, Diff.empty, 0L, 0L, Set.empty, ConsensusPostActionDiff.empty).bestLiquidBlock.uniqueId shouldBe block.uniqueId
    }
  }

  property("can resolve best last block") {
    forAll(preconditionsAndPayments(5)) {
      case (genesis, payments) =>
        val microInfos = payments.zipWithIndex.map {
          case (tx, idx) =>
            MicroInfoForChain(Seq(tx), microTimestamp = 1000 + idx * 50)
        }

        val (block, microBlocks) = chainBaseAndMicro(randomSig, Seq(genesis), microInfos, genesis.timestamp, defaultSigner)

        val ng = new NgState(block, Liquid, Diff.empty, 0L, 0L, Set.empty, ConsensusPostActionDiff.empty)

        microBlocks.foldLeft(1000) {
          case (thisTime, m) =>
            ng.append(m, Diff.empty, 0L, 0L, CertChainStore.empty, Set.empty)
            thisTime + 50
        }

        ng.bestLastBlockInfo(0).blockId shouldBe block.uniqueId
        ng.bestLastBlockInfo(1001).blockId shouldBe microBlocks.head.totalLiquidBlockSig
        ng.bestLastBlockInfo(1051).blockId shouldBe microBlocks.tail.head.totalLiquidBlockSig
        ng.bestLastBlockInfo(2000).blockId shouldBe microBlocks.last.totalLiquidBlockSig

        new NgState(block, Liquid, Diff.empty, 0L, 0L, Set.empty, ConsensusPostActionDiff.empty).bestLiquidBlock.uniqueId shouldBe block.uniqueId
    }
  }

  property("calculates carry fee correctly") {
    forAll(preconditionsAndPayments(5)) {
      case (genesis, payments) =>
        val (block, microBlocks) = chainBaseAndMicro(randomSig, Seq(genesis), payments.map(t => Seq(t)))

        val ng = new NgState(block, Liquid, Diff.empty, 0L, 0L, Set.empty, ConsensusPostActionDiff.empty)
        microBlocks.foreach(m => ng.append(m, Diff.empty, 1L, 0L, CertChainStore.empty, Set.empty))

        ng.totalDiffOf(block.uniqueId).map(_._3) shouldBe Some(0L)
        microBlocks.zipWithIndex.foreach {
          case (m, i) =>
            val u = ng.totalDiffOf(m.totalLiquidBlockSig).map(_._3)
            u shouldBe Some(i + 1)
        }
        ng.carryFee shouldBe microBlocks.size
    }
  }
}
