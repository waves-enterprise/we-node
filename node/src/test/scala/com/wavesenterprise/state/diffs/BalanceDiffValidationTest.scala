package com.wavesenterprise.state.diffs

import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.GenesisTransaction
import com.wavesenterprise.transaction.lease.LeaseTransaction
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class BalanceDiffValidationTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  val ownLessThatLeaseOut: Gen[(GenesisTransaction, TransferTransactionV2, LeaseTransaction, LeaseTransaction, TransferTransactionV2)] = for {
    master <- accountGen
    alice  <- accountGen
    bob    <- accountGen
    cooper <- accountGen
    ts     <- ntpTimestampGen
    amt    <- positiveLongGen
    fee    <- smallFeeGen
    genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
    masterTransfersToAlice      = createWestTransfer(master, alice.toAddress, amt, fee, ts).explicitGet()
    (aliceLeasesToBob, _)    <- leaseAndCancelGeneratorP(alice, bob.toAddress, alice) suchThat (_._1.amount < amt)
    (masterLeasesToAlice, _) <- leaseAndCancelGeneratorP(master, alice.toAddress, master) suchThat (_._1.amount > aliceLeasesToBob.amount)
    transferAmt              <- Gen.choose(amt - fee - aliceLeasesToBob.amount, amt - fee)
    aliceTransfersMoreThanOwnsMinusLeaseOut = createWestTransfer(alice, cooper.toAddress, transferAmt, fee, ts).explicitGet()

  } yield (genesis, masterTransfersToAlice, aliceLeasesToBob, masterLeasesToAlice, aliceTransfersMoreThanOwnsMinusLeaseOut)

  property("cannot transfer more than own-leaseOut after allow-leased-balance-transfer-until") {
    forAll(ownLessThatLeaseOut) {
      case (genesis, masterTransfersToAlice, aliceLeasesToBob, masterLeasesToAlice, aliceTransfersMoreThanOwnsMinusLeaseOut) =>
        assertDiffEi(
          Seq(
            TestBlock.create(Seq(genesis)),
            TestBlock.create(Seq()),
            TestBlock.create(Seq()),
            TestBlock.create(Seq()),
            TestBlock.create(Seq(masterTransfersToAlice, aliceLeasesToBob, masterLeasesToAlice))
          ),
          TestBlock.create(Seq(aliceTransfersMoreThanOwnsMinusLeaseOut))
        ) { totalDiffEi =>
          totalDiffEi should produce("trying to spend leased money")
        }
    }
  }
}
