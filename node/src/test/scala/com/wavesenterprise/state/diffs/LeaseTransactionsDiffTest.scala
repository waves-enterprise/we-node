package com.wavesenterprise.state.diffs

import cats._
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.TestFunctionalitySettings
import com.wavesenterprise.state._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.GenesisTransaction
import com.wavesenterprise.transaction.lease.{LeaseCancelTransaction, LeaseTransaction}
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class LeaseTransactionsDiffTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  private val allowMultipleLeaseCancelTransactionUntilTimestamp = Long.MaxValue / 2
  private val settings                                          = TestFunctionalitySettings.Enabled

  def total(l: LeaseBalance): Long = l.in - l.out

  property("can lease/cancel lease preserving WEST invariant") {

    val sunnyDayLeaseLeaseCancel: Gen[(GenesisTransaction, LeaseTransaction, LeaseCancelTransaction)] = for {
      master    <- accountGen
      recipient <- accountGen suchThat (_ != master)
      ts        <- ntpTimestampGen
      genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      (lease, unlease) <- leaseAndCancelGeneratorP(master, recipient.toAddress, master)
    } yield (genesis, lease, unlease)

    forAll(sunnyDayLeaseLeaseCancel) {
      case (genesis, lease, leaseCancel) =>
        assertDiffAndState(Seq(TestBlock.create(Seq(genesis))), TestBlock.create(Seq(lease))) {
          case (totalDiff, newState) =>
            val totalPortfolioDiff = Monoid.combineAll(totalDiff.portfolios.values)
            totalPortfolioDiff.balance shouldBe 0
            total(totalPortfolioDiff.lease) shouldBe 0
            totalPortfolioDiff.effectiveBalance shouldBe 0
            totalPortfolioDiff.assets.values.foreach(_ shouldBe 0)
        }

        assertDiffAndState(Seq(TestBlock.create(Seq(genesis, lease))), TestBlock.create(Seq(leaseCancel))) {
          case (totalDiff, newState) =>
            val totalPortfolioDiff = Monoid.combineAll(totalDiff.portfolios.values)
            totalPortfolioDiff.balance shouldBe 0
            total(totalPortfolioDiff.lease) shouldBe 0
            totalPortfolioDiff.effectiveBalance shouldBe 0
            totalPortfolioDiff.assets.values.foreach(_ shouldBe 0)
        }
    }
  }

  val cancelLeaseTwice: Gen[(GenesisTransaction, TransferTransactionV2, LeaseTransaction, LeaseCancelTransaction, LeaseCancelTransaction)] = for {
    master    <- accountGen
    recipient <- accountGen suchThat (_ != master)
    ts        <- ntpTimestampGen
    genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts - 2000).explicitGet()
    (lease, unlease) <- leaseAndCancelGeneratorP(master, recipient.toAddress, master)
    fee2             <- smallFeeGen
    unlease2         <- createLeaseCancel(master, lease.id(), fee2, ts + 1)
    // ensure recipient has enough effective balance
    payment <- westTransferGeneratorP(master, recipient.toAddress) suchThat (_.amount > lease.amount)
  } yield (genesis, payment, lease, unlease, unlease2)

  property("cannot cancel lease twice") {
    forAll(cancelLeaseTwice, timestampGen retryUntil (_ > allowMultipleLeaseCancelTransactionUntilTimestamp)) {
      case ((genesis, payment, lease, leaseCancel, leaseCancel2), blockTime) =>
        assertDiffEi(Seq(TestBlock.create(Seq(genesis, payment, lease, leaseCancel))), TestBlock.create(blockTime, Seq(leaseCancel2)), settings) {
          totalDiffEi =>
            totalDiffEi should produce("Cannot cancel already cancelled lease")
        }
    }
  }

  property("cannot lease more than actual balance(cannot lease forward)") {
    val setup: Gen[(GenesisTransaction, LeaseTransaction, LeaseTransaction)] = for {
      master    <- accountGen
      recipient <- accountGen suchThat (_ != master)
      forward   <- accountGen suchThat (!Set(master, recipient).contains(_))
      ts        <- ntpTimestampGen
      genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts - 2000).explicitGet()
      (lease, _)        <- leaseAndCancelGeneratorP(master, recipient.toAddress, master)
      (leaseForward, _) <- leaseAndCancelGeneratorP(recipient, forward.toAddress, recipient)
    } yield (genesis, lease, leaseForward)

    forAll(setup) {
      case (genesis, lease, leaseForward) =>
        assertDiffEi(Seq(TestBlock.create(Seq(genesis, lease))), TestBlock.create(Seq(leaseForward)), settings) { totalDiffEi =>
          totalDiffEi should produce("Cannot lease more than own")
        }
    }
  }

  def cancelLeaseOfAnotherSender(
      unleaseByRecipient: Boolean): Gen[(GenesisTransaction, GenesisTransaction, LeaseTransaction, LeaseCancelTransaction)] =
    for {
      master    <- accountGen
      recipient <- accountGen suchThat (_ != master)
      other     <- accountGen suchThat (_ != recipient)
      unleaser = if (unleaseByRecipient) recipient else other
      ts <- ntpTimestampGen
      genesis: GenesisTransaction  = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts - 5000).explicitGet()
      genesis2: GenesisTransaction = GenesisTransaction.create(unleaser.toAddress, ENOUGH_AMT, ts - 6000).explicitGet()
      (lease, _)              <- leaseAndCancelGeneratorP(master, recipient.toAddress, master)
      fee2                    <- smallFeeGen
      unleaseOtherOrRecipient <- createLeaseCancel(unleaser, lease.id(), fee2, ts + 1)
    } yield (genesis, genesis2, lease, unleaseOtherOrRecipient)

  property("cannot cancel lease of another sender after allowMultipleLeaseCancelTransactionUntilTimestamp") {
    forAll(Gen.oneOf(true, false).flatMap(cancelLeaseOfAnotherSender),
           timestampGen retryUntil (_ > allowMultipleLeaseCancelTransactionUntilTimestamp)) {
      case ((genesis, genesis2, lease, unleaseOtherOrRecipient), blockTime) =>
        assertDiffEi(Seq(TestBlock.create(Seq(genesis, genesis2, lease))), TestBlock.create(blockTime, Seq(unleaseOtherOrRecipient)), settings) {
          totalDiffEi =>
            totalDiffEi should produce("LeaseTransaction was leased by other sender")
        }
    }
  }
}
