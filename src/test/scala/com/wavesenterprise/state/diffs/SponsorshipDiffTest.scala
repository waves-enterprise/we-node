package com.wavesenterprise.state.diffs

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock.{create => block}
import com.wavesenterprise.settings.TestFunctionalitySettings
import com.wavesenterprise.state._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.GenesisTransaction
import com.wavesenterprise.transaction.assets.{IssueTransactionV2, SponsorFeeTransactionV1}
import com.wavesenterprise.transaction.lease.LeaseTransactionV2
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.utils.Base58
import com.wavesenterprise.utils.NumberUtils.DoubleExt
import org.scalatest.{Matchers, PropSpec}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SponsorshipDiffTest extends PropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen {

  def settings(sponsorshipActivationHeight: Int) =
    TestFunctionalitySettings.Enabled.copy(
      preActivatedFeatures = Map(
        BlockchainFeature.FeeSwitch.id            -> sponsorshipActivationHeight,
        BlockchainFeature.SmartAccounts.id        -> 0,
        BlockchainFeature.SponsoredFeesSupport.id -> sponsorshipActivationHeight
      ),
      featureCheckBlocksPeriod = 2,
      blocksForFeatureActivation = 1
    )

  property("work") {
    val s = settings(0)
    val setup = for {
      master <- accountGen
      ts     <- timestampGen
      genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      (issueTx, sponsorTx, sponsor1Tx, cancelTx) <- sponsorFeeCancelSponsorFeeGen(master)
    } yield (genesis, issueTx, sponsorTx, sponsor1Tx, cancelTx)

    forAll(setup) {
      case (genesis, issue, sponsor, sponsor1, cancel) =>
        val setupBlocks = Seq(block(Seq(genesis, issue)))
        assertDiffAndState(setupBlocks, block(Seq(sponsor)), s) {
          case (diff, state) =>
            diff.sponsorship shouldBe Map(sponsor.assetId -> SponsorshipValue(true))
            state.assetDescription(sponsor.assetId).map(_.sponsorshipIsEnabled) shouldBe Some(true)
        }
        assertDiffAndState(setupBlocks, block(Seq(sponsor, sponsor1)), s) {
          case (diff, state) =>
            diff.sponsorship shouldBe Map(sponsor.assetId -> SponsorshipValue(true))
            state.assetDescription(sponsor.assetId).map(_.sponsorshipIsEnabled) shouldBe Some(true)
        }
        assertDiffAndState(setupBlocks, block(Seq(sponsor, sponsor1, cancel)), s) {
          case (diff, state) =>
            diff.sponsorship shouldBe Map(sponsor.assetId -> SponsorshipValue(false))
            state.assetDescription(sponsor.assetId).map(_.sponsorshipIsEnabled) shouldBe Some(false)
        }
    }
  }

  property("validation fails if asset doesn't exist") {
    val s = settings(0)
    val setup = for {
      master <- accountGen
      ts     <- timestampGen
      genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      (_, sponsorTx, _, cancelTx) <- sponsorFeeCancelSponsorFeeGen(master)
    } yield (genesis, sponsorTx, cancelTx)

    forAll(setup) {
      case (genesis, sponsor, cancel) =>
        val setupBlocks = Seq(block(Seq(genesis)))
        assertDiffEi(setupBlocks, block(Seq(sponsor)), s) { blockDiffEi =>
          blockDiffEi should produce("Referenced assetId not found")
        }
        assertDiffEi(setupBlocks, block(Seq(cancel)), s) { blockDiffEi =>
          blockDiffEi should produce("Referenced assetId not found")
        }
    }
  }

  property("validation fails prior to feature activation") {
    val s = settings(100)
    val setup = for {
      master <- accountGen
      ts     <- timestampGen
      genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      (issueTx, sponsorTx, _, _) <- sponsorFeeCancelSponsorFeeGen(master)
    } yield (genesis, issueTx, sponsorTx)

    forAll(setup) {
      case (genesis, issue, sponsor) =>
        val setupBlocks = Seq(block(Seq(genesis, issue)))
        assertDiffEi(setupBlocks, block(Seq(sponsor)), s) { blockDiffEi =>
          blockDiffEi should produce("has not been activated")
        }
    }
  }

  property("not enough fee") {
    val s = settings(0)
    val setup = for {
      master <- accountGen
      ts     <- timestampGen
      initBalance                 = 400000000
      genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, initBalance, ts).explicitGet()
      (issueTx, sponsorTx, _, _) <- sponsorFeeCancelSponsorFeeGen(master)
      recipient                  <- accountGen
      assetId       = issueTx.id()
      baseUnitRatio = 1
      assetOverspend = TransferTransactionV2
        .selfSigned(master, None, Some(assetId), ts + 1, 1000000, issueTx.quantity + 1, recipient.toAddress, Array.emptyByteArray)
        .right
        .get
      insufficientFee = TransferTransactionV2
        .selfSigned(master, None, Some(assetId), ts + 2, 1000000, 1, recipient.toAddress, Array.emptyByteArray)
        .right
        .get
      fee = initBalance * baseUnitRatio + 1
      westOverspend = TransferTransactionV2
        .selfSigned(master, None, Some(assetId), ts + 3, 1000000, fee, recipient.toAddress, Array.emptyByteArray)
        .right
        .get
    } yield (genesis, issueTx, sponsorTx, assetOverspend, insufficientFee, westOverspend)

    forAll(setup) {
      case (genesis, issue, sponsor, assetOverspend, insufficientFee, westOverspend) =>
        val setupBlocks = Seq(block(Seq(genesis, issue, sponsor)))
        assertDiffEi(setupBlocks, block(Seq(assetOverspend)), s) { blockDiffEi =>
          blockDiffEi should produce("unavailable funds")
        }
        assertDiffEi(setupBlocks, block(Seq(insufficientFee)), s) { blockDiffEi =>
          blockDiffEi should produce("does not exceed minimal")
        }
        assertDiffEi(setupBlocks, block(Seq(westOverspend)), s) { blockDiffEi =>
          if (westOverspend.fee > issue.quantity)
            blockDiffEi should produce("unavailable funds")
          else
            blockDiffEi should produce("negative WEST balance")
        }
    }
  }

  property("not enough WEST to pay fee after leasing") {
    val s = settings(0)
    val setup = for {
      master <- accountGen
      alice  <- accountGen
      bob    <- accountGen
      ts     <- timestampGen
      fee    <- smallFeeGen
      amount                       = ENOUGH_AMT / 2
      genesis: GenesisTransaction  = GenesisTransaction.create(master.toAddress, amount, ts).explicitGet()
      genesis2: GenesisTransaction = GenesisTransaction.create(bob.toAddress, amount, ts).explicitGet()
      (issueTx, sponsorTx, _, _) <- sponsorFeeCancelSponsorFeeGen(master)
      assetId       = issueTx.id()
      baseUnitRatio = 1
      transferAssetTx = TransferTransactionV2
        .selfSigned(master, Some(assetId), None, ts + 2, issueTx.quantity, fee, alice.toAddress, Array.emptyByteArray)
        .right
        .get
      leasingTx = LeaseTransactionV2
        .selfSigned(None, master, bob.toAddress, amount - issueTx.fee - sponsorTx.fee - 2 * fee, fee, ts + 3)
        .right
        .get
      leasingToMasterTx = LeaseTransactionV2
        .selfSigned(None, bob, master.toAddress, amount / 2, fee, ts + 3)
        .right
        .get
      insufficientFee = TransferTransactionV2
        .selfSigned(alice, Some(assetId), Some(assetId), ts + 4, issueTx.quantity / 12, fee * baseUnitRatio, bob.toAddress, Array.emptyByteArray)
        .right
        .get
    } yield (genesis, genesis2, issueTx, sponsorTx, transferAssetTx, leasingTx, insufficientFee, leasingToMasterTx)

    forAll(setup) {
      case (genesis, genesis2, issueTx, sponsorTx, transferAssetTx, leasingTx, insufficientFee, leasingToMaster) =>
        val setupBlocks = Seq(block(Seq(genesis, genesis2, issueTx, sponsorTx)), block(Seq(transferAssetTx, leasingTx)))
        assertDiffEi(setupBlocks, block(Seq(insufficientFee)), s) { blockDiffEi =>
          blockDiffEi should produce("negative effective balance")
        }
        assertDiffEi(setupBlocks, block(Seq(leasingToMaster, insufficientFee)), s) { blockDiffEi =>
          blockDiffEi should produce("trying to spend leased money")
        }
    }
  }

  property("cannot cancel sponsorship") {
    val s = settings(0)
    val setup = for {
      master     <- accountGen
      notSponsor <- accountGen
      ts         <- timestampGen
      genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, 400000000, ts).explicitGet()
      (issueTx, sponsorTx, _, _) <- sponsorFeeCancelSponsorFeeGen(master)
      recipient                  <- accountGen
      assetId = issueTx.id()
      senderNotIssuer = SponsorFeeTransactionV1
        .selfSigned(notSponsor, assetId, false, 1.west, ts + 1)
        .right
        .get
      insufficientFee = SponsorFeeTransactionV1
        .selfSigned(notSponsor, assetId, false, 1.west - 1, ts + 1)
        .right
        .get
    } yield (genesis, issueTx, sponsorTx, senderNotIssuer, insufficientFee)

    forAll(setup) {
      case (genesis, issueTx, sponsorTx, senderNotIssuer, insufficientFee) =>
        val setupBlocks = Seq(block(Seq(genesis, issueTx, sponsorTx)))
        assertDiffEi(setupBlocks, block(Seq(senderNotIssuer)), s) { blockDiffEi =>
          blockDiffEi should produce("Asset was issued by other address")
        }
        assertDiffEi(setupBlocks, block(Seq(insufficientFee)), s) { blockDiffEi =>
          blockDiffEi should produce("does not exceed minimal value of")
        }
    }
  }

  property("cannot сhange sponsorship fee") {
    val s = settings(0)
    val setup = for {
      master     <- accountGen
      notSponsor <- accountGen
      ts         <- timestampGen
      genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, 400000000, ts).explicitGet()
      (issueTx, sponsorTx, _, _) <- sponsorFeeCancelSponsorFeeGen(master)
      recipient                  <- accountGen
      assetId = issueTx.id()
      senderNotIssuer = SponsorFeeTransactionV1
        .selfSigned(notSponsor, assetId, isEnabled = true, 1.west, ts + 1)
        .right
        .get
      insufficientFee = SponsorFeeTransactionV1
        .selfSigned(master, assetId, isEnabled = true, 1.west - 1, ts + 1)
        .right
        .get
    } yield (genesis, issueTx, sponsorTx, senderNotIssuer, insufficientFee)

    forAll(setup) {
      case (genesis, issueTx, sponsorTx, senderNotIssuer, insufficientFee) =>
        val setupBlocks = Seq(block(Seq(genesis, issueTx, sponsorTx)))
        assertDiffEi(setupBlocks, block(Seq(senderNotIssuer)), s) { blockDiffEi =>
          blockDiffEi should produce("Asset was issued by other address")
        }
        assertDiffEi(setupBlocks, block(Seq(insufficientFee)), s) { blockDiffEi =>
          blockDiffEi should produce("does not exceed minimal value of")
        }
    }
  }

  property("sponsor has no WEST but receives them just in time") {
    val s = settings(0)
    val setup = for {
      master    <- accountGen
      recipient <- accountGen
      ts        <- timestampGen
      genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, 300000000, ts).explicitGet()
      issue = IssueTransactionV2
        .selfSigned(
          chainId = currentChainId,
          sender = master,
          name = Base58.decode("Asset").get,
          description = Array.emptyByteArray,
          quantity = 1000000,
          decimals = 2,
          reissuable = false,
          fee = 100000000,
          timestamp = ts + 1,
          script = None
        )
        .explicitGet()
      assetId = issue.id()
      sponsor = SponsorFeeTransactionV1.selfSigned(master, assetId, isEnabled = true, 100000000, ts + 2).explicitGet()
      assetTransfer = TransferTransactionV2
        .selfSigned(master, Some(assetId), None, ts + 3, issue.quantity, 100000, recipient.toAddress, Array.emptyByteArray)
        .right
        .get
      westTransfer = TransferTransactionV2
        .selfSigned(master, None, None, ts + 4, 99800000, 100000, recipient.toAddress, Array.emptyByteArray)
        .right
        .get
      backWestTransfer = TransferTransactionV2
        .selfSigned(recipient, None, Some(assetId), ts + 5, 1000000, 1000000, master.toAddress, Array.emptyByteArray)
        .right
        .get
    } yield (genesis, issue, sponsor, assetTransfer, westTransfer, backWestTransfer)

    forAll(setup) {
      case (genesis, issue, sponsor, assetTransfer, westTransfer, backWestTransfer) =>
        assertDiffAndState(Seq(block(Seq(genesis, issue, sponsor, assetTransfer, westTransfer))), block(Seq(backWestTransfer)), s) {
          case (diff, state) =>
            val portfolio = state.portfolio(genesis.recipient)
            portfolio.balance shouldBe 0
            portfolio.assets(issue.id()) shouldBe issue.quantity
        }
    }
  }

}
