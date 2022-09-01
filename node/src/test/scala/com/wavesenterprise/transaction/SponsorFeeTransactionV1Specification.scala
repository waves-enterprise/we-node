package com.wavesenterprise.transaction

import com.wavesenterprise.TransactionGen
import com.wavesenterprise.features.BlockchainFeature._
import com.wavesenterprise.lagonaki.mocks.TestBlock.{create => block}
import com.wavesenterprise.settings.{FunctionalitySettings, TestFunctionalitySettings}
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.transaction.assets.{IssueTransactionV2, SponsorFeeTransactionV1}
import com.wavesenterprise.transaction.transfer.TransferTransactionV2
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.scalatest._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class SponsorFeeTransactionV1Specification
    extends AnyPropSpec
    with ScalaCheckPropertyChecks
    with Matchers
    with TransactionGen
    with WithSenderAndRecipient {

  val One = 100000000L
  val NgAndSponsorshipSettings: FunctionalitySettings = TestFunctionalitySettings.Enabled
    .copy(
      preActivatedFeatures = Map(
        NG.id                   -> 0,
        FeeSwitch.id            -> 0,
        SmartAccounts.id        -> 0,
        SponsoredFeesSupport.id -> 0
      ),
      blocksForFeatureActivation = 1,
      featureCheckBlocksPeriod = 2
    )

  property("miner receives one satoshi less than sponsor pays") {
    val setup = for {
      (acc, name, desc, quantity, decimals, reissuable, fee, ts) <- issueParamGen
      genesis    = GenesisTransaction.create(acc.toAddress, ENOUGH_AMT, ts).explicitGet()
      issue      = IssueTransactionV2.selfSigned(currentChainId, acc, name, desc, quantity, decimals, reissuable, fee, ts, None).explicitGet()
      assetRatio = 1
      fee <- smallFeeGen
      sponsor = SponsorFeeTransactionV1.selfSigned(acc, issue.id(), isEnabled = true, fee = One, timestamp = ts).explicitGet()
      transfer = TransferTransactionV2
        .selfSigned(sender = acc,
                    assetId = None,
                    feeAssetId = Some(issue.id()),
                    timestamp = ts,
                    amount = 1,
                    fee = fee * assetRatio,
                    recipient = acc.toAddress,
                    attachment = Array())
        .explicitGet()
    } yield (acc, genesis, issue, sponsor, transfer)

    forAll(setup) {
      case (acc, genesis, issue, sponsor, transfer) =>
        val b0 = block(acc, Seq(genesis, issue, sponsor))
        val b1 = block(acc, Seq(transfer))
        val b2 = block(acc, Seq.empty)

        assertNgDiffState(Seq(b0, b1), b2, NgAndSponsorshipSettings) {
          case (_, state) =>
            state.balance(acc.toAddress, None) - ENOUGH_AMT shouldBe 0
        }
    }
  }

  property("miner receives one satoshi more than sponsor pays") {
    val setup = for {
      (acc, name, desc, quantity, decimals, reissuable, fee, ts) <- issueParamGen
      genesis    = GenesisTransaction.create(acc.toAddress, ENOUGH_AMT, ts).explicitGet()
      issue      = IssueTransactionV2.selfSigned(currentChainId, acc, name, desc, quantity, decimals, reissuable, fee, ts, None).explicitGet()
      assetRatio = 1
      sponsor    = SponsorFeeTransactionV1.selfSigned(acc, issue.id(), isEnabled = true, One, ts).explicitGet()
      fee <- smallFeeGen
      transfer1 = TransferTransactionV2
        .selfSigned(acc, None, feeAssetId = Some(issue.id()), ts, 1, fee * assetRatio + 7, acc.toAddress, Array())
        .explicitGet()
      transfer2 = TransferTransactionV2
        .selfSigned(acc, None, feeAssetId = Some(issue.id()), ts, 1, fee * assetRatio + 9, acc.toAddress, Array())
        .explicitGet()
    } yield (acc, genesis, issue, sponsor, transfer1, transfer2)

    forAll(setup) {
      case (acc, genesis, issue, sponsor, transfer1, transfer2) =>
        val b0 = block(acc, Seq(genesis, issue, sponsor))
        val b1 = block(acc, Seq(transfer1, transfer2))
        val b2 = block(acc, Seq.empty)

        assertNgDiffState(Seq(b0, b1), b2, NgAndSponsorshipSettings) {
          case (diff, state) =>
            state.balance(acc.toAddress, None) - ENOUGH_AMT shouldBe 0
        }
    }
  }

  property("sponsorship changes in the middle of a block") {
    val setup = for {
      (acc, name, desc, quantity, decimals, reissuable, fee, ts) <- issueParamGen
      genesis    = GenesisTransaction.create(acc.toAddress, ENOUGH_AMT, ts).explicitGet()
      issue      = IssueTransactionV2.selfSigned(currentChainId, acc, name, desc, quantity, decimals, reissuable, fee, ts, None).explicitGet()
      assetRatio = 1
      fee <- smallFeeGen
      sponsor1 = SponsorFeeTransactionV1.selfSigned(acc, issue.id(), isEnabled = true, One, ts).explicitGet()
      transfer1 = TransferTransactionV2
        .selfSigned(acc, None, Some(issue.id()), ts + 1, 1, fee = fee * assetRatio, acc.toAddress, Array())
        .explicitGet()
      assetRatio2 = 1
      sponsor2    = SponsorFeeTransactionV1.selfSigned(acc, issue.id(), isEnabled = true, One, ts + 2).explicitGet()
      transfer2 = TransferTransactionV2
        .selfSigned(acc, None, Some(issue.id()), ts + 3, 1, fee = fee * assetRatio2, acc.toAddress, Array())
        .explicitGet()
    } yield (acc, genesis, issue, sponsor1, transfer1, sponsor2, transfer2)

    forAll(setup) {
      case (acc, genesis, issue, sponsor1, transfer1, sponsor2, transfer2) =>
        val b0 = block(acc, Seq(genesis, issue, sponsor1))
        val b1 = block(acc, Seq(transfer1, sponsor2, transfer2))
        val b2 = block(acc, Seq.empty)

        assertNgDiffState(Seq(b0, b1), b2, NgAndSponsorshipSettings) {
          case (diff, state) =>
            state.balance(acc.toAddress, None) - ENOUGH_AMT shouldBe 0
        }
    }
  }
}
