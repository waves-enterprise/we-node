package com.wavesenterprise.state.diffs

import cats.implicits._
import com.wavesenterprise.state.{LeaseBalance, Portfolio}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import com.wavesenterprise.account.Address
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.{FunctionalitySettings, TestFunctionalitySettings}
import com.wavesenterprise.state.diffs.smart.smartEnabledFS
import com.wavesenterprise.transaction.GenesisTransaction
import com.wavesenterprise.transaction.ValidationError.GenericError
import com.wavesenterprise.transaction.assets._
import com.wavesenterprise.transaction.transfer._
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class TransferTransactionDiffTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  val fs: FunctionalitySettings = TestFunctionalitySettings.Enabled.copy(
    preActivatedFeatures = TestFunctionalitySettings.Enabled.preActivatedFeatures ++ Map(BlockchainFeature.SponsoredFeesSupport.id -> 0))

  val preconditionsAndTransfer: Gen[(GenesisTransaction, IssueTransaction, IssueTransaction, TransferTransaction)] = for {
    master    <- accountGen
    recepient <- otherAccountGen(candidate = master)
    ts        <- positiveIntGen
    genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
    issue1: IssueTransaction <- issueReissueBurnGeneratorP(ENOUGH_AMT, master).map(_._1)
    issue2: IssueTransaction <- issueReissueBurnGeneratorP(ENOUGH_AMT, master).map(_._1)
    maybeAsset               <- Gen.option(issue1)
    maybeAsset2              <- Gen.option(issue2)
    maybeFeeAsset            <- Gen.oneOf(maybeAsset, maybeAsset2)
    transferV1               <- transferGeneratorP(master, recepient.toAddress, maybeAsset.map(_.id()), maybeFeeAsset.map(_.id()))
    transferV2               <- versionedTransferGeneratorP(master, recepient.toAddress, maybeAsset.map(_.id()), maybeFeeAsset.map(_.id()))
    transfer                 <- Gen.oneOf(transferV1, transferV2)
  } yield (genesis, issue1, issue2, transfer)

  property("transfers assets to recipient preserving WEST invariant") {
    forAll(preconditionsAndTransfer) {
      case ((genesis, issue1, issue2, transfer)) =>
        assertDiffAndState(Seq(TestBlock.create(Seq(genesis, issue1, issue2))), TestBlock.create(Seq(transfer)), fs) {
          case (totalDiff, newState) =>
            assertBalanceInvariant(totalDiff)

            val recipient: Address = transfer.recipient.asInstanceOf[Address]
            val recipientPortfolio = newState.portfolio(recipient)
            if (transfer.sender.toAddress != recipient) {
              transfer.assetId match {
                case Some(aid) => recipientPortfolio shouldBe Portfolio(0, LeaseBalance.empty, Map(aid -> transfer.amount))
                case None      => recipientPortfolio shouldBe Portfolio(transfer.amount, LeaseBalance.empty, Map.empty)
              }
            }
        }
    }
  }

  val transferWithSmartAssetFee: Gen[(GenesisTransaction, IssueTransaction, IssueTransactionV2, TransferTransaction)] = {
    for {
      master    <- accountGen
      recepient <- otherAccountGen(master)
      ts        <- positiveIntGen
      genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      issue: IssueTransaction      <- issueReissueBurnGeneratorP(ENOUGH_AMT, master).map(_._1)
      feeIssue: IssueTransactionV2 <- smartIssueTransactionGen(master, scriptGen.map(_.some))
      transfer                     <- transferGeneratorP(master, recepient.toAddress, issue.id().some, feeIssue.id().some)
    } yield (genesis, issue, feeIssue, transfer)
  }

  val fsWithSmartEnabled: FunctionalitySettings =
    smartEnabledFS.copy(preActivatedFeatures = smartEnabledFS.preActivatedFeatures ++ Map(BlockchainFeature.SponsoredFeesSupport.id -> 0))

  property("fails, if smart asset used as a fee") {

    forAll(transferWithSmartAssetFee) {
      case (genesis, issue, fee, transfer) =>
        assertDiffAndState(Seq(TestBlock.create(Seq(genesis))), TestBlock.create(Seq(issue, fee)), smartEnabledFS) {
          case (_, state) =>
            val diffOrError = TransferTransactionDiff(state, smartEnabledFS, System.currentTimeMillis(), state.height)(transfer)
            diffOrError shouldBe Left(GenericError("Smart assets can't participate in TransferTransactions as a fee"))
        }
    }
  }
}
