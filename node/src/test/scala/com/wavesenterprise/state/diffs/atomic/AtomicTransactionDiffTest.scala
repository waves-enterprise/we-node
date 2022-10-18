package com.wavesenterprise.state.diffs.atomic

import cats.kernel.Monoid
import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.block.Block
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.{FunctionalitySettings, TestFunctionalitySettings}
import com.wavesenterprise.state.diffs.{ENOUGH_AMT, assertDiffAndState, assertLeft}
import com.wavesenterprise.state.Portfolio
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.transfer.{TransferTransaction, TransferTransactionV3}
import com.wavesenterprise.transaction.{AtomicTransaction, AtomicUtils, GenesisTransaction}
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class AtomicTransactionDiffTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  private val transferAmount = ENOUGH_AMT / 1000000
  private val senderBalance  = ENOUGH_AMT / 1000
  private val transfersCount = 10

  private def transferGen(tsGen: Gen[Long], trustedAddress: Option[Address]): Gen[TransferTransactionV3] = {
    for {
      transferTime      <- tsGen
      transferSigner    <- accountGen
      transferRecipient <- accountGen
      transferTx        <- transferGeneratorPV3(transferTime, transferSigner, transferRecipient.toAddress, transferAmount, trustedAddress)
    } yield transferTx
  }

  val fs: FunctionalitySettings = TestFunctionalitySettings.EnabledForAtomics

  property("check AtomicTransactionV1 diff and state") {
    val preconditions: Gen[(Block, PrivateKeyAccount, AtomicTransaction)] = for {
      genesisTime  <- ntpTimestampGen.map(_ - 5.minute.toMillis)
      atomicMiner  <- accountGen
      atomicSigner <- accountGen
      transferTxs  <- Gen.listOfN(transfersCount, transferGen(ntpTimestampGen.map(_ - 2.minute.toMillis), Some(atomicSigner.toAddress)))
      atomicTx     <- atomicTxV1Gen(atomicSigner, transferTxs, ntpTimestampGen.map(_ - 1.minute.toMillis))
      signedAtomic = AtomicUtils.addMinerProof(atomicTx, atomicMiner).explicitGet()
      genesisTxs   = (signedAtomic +: transferTxs).map(tx => GenesisTransaction.create(tx.sender.toAddress, senderBalance, genesisTime).explicitGet())
      genesisBlock = TestBlock.create(genesisTxs)
    } yield (genesisBlock, atomicMiner, signedAtomic)

    forAll(preconditions) {
      case (genesisBlock, atomicMiner, atomicTx) =>
        assertDiffAndState(Seq(genesisBlock), TestBlock.create(atomicMiner, Seq(atomicTx)), fs) {
          case (blockDiff, state) =>
            val totalPortfolioDiff: Portfolio = Monoid.combineAll(blockDiff.portfolios.values)
            totalPortfolioDiff.balance shouldBe 0
            totalPortfolioDiff.effectiveBalance shouldBe 0
            totalPortfolioDiff.assets shouldBe Map.empty

            blockDiff.transactions.size shouldBe (atomicTx.transactions.size + 1)
            blockDiff.transactionsMap.get(atomicTx.id()).map(_._2) shouldBe Some(atomicTx)
            state.transactionInfo(atomicTx.id()).map(_._2) shouldBe Some(atomicTx)

            atomicTx.transactions.foreach {
              case tx: TransferTransaction =>
                blockDiff.transactionsMap.get(tx.id()).map(_._2) shouldBe Some(tx)
                state.transactionInfo(tx.id()).map(_._2) shouldBe Some(tx)

                val recipient: Address = tx.recipient.asInstanceOf[Address]
                state.addressBalance(recipient) shouldBe tx.amount
            }
        }
    }
  }

  property("transactions with atomic badge cannot be mined outside of atomic container") {
    val preconditions: Gen[(Block, PrivateKeyAccount, TransferTransactionV3)] = for {
      genesisTime         <- ntpTimestampGen.map(_ - 5.minute.toMillis)
      miner               <- accountGen
      trustedSender       <- accountGen
      transferTxWithBadge <- transferGen(ntpTimestampGen.map(_ - 2.minute.toMillis), Some(trustedSender.toAddress))
      genesisTx    = GenesisTransaction.create(transferTxWithBadge.sender.toAddress, senderBalance, genesisTime).explicitGet()
      genesisBlock = TestBlock.create(Seq(genesisTx))
    } yield (genesisBlock, miner, transferTxWithBadge)

    forAll(preconditions) {
      case (genesisBlock, miner, transferTx) =>
        assertLeft(Seq(genesisBlock), TestBlock.create(miner, Seq(transferTx)), fs) {
          "with atomic badge cannot be mined outside of atomic container"
        }
    }
  }
}
