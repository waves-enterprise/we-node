package com.wavesenterprise.state.diffs

import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock.{create => block}
import com.wavesenterprise.settings.TestFunctionalitySettings
import com.wavesenterprise.state.{LeaseBalance, Portfolio}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.GenesisTransaction
import com.wavesenterprise.transaction.transfer.ParsedTransfer
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import com.wavesenterprise.transaction.validation.TransferValidation.MaxTransferCount
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class MassTransferTransactionDiffTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  val fs = TestFunctionalitySettings.Enabled.copy(
    preActivatedFeatures = Map(BlockchainFeature.MassTransfer.id -> 0, BlockchainFeature.SmartAccounts.id -> 0))

  val baseSetup: Gen[(GenesisTransaction, PrivateKeyAccount)] = for {
    master <- accountGen
    ts     <- positiveLongGen
    genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
  } yield (genesis, master)

  property("MassTransfer preserves balance invariant") {
    def testDiff(transferCount: Int): Unit = {
      val setup = for {
        (genesis, master) <- baseSetup
        transferGen = for {
          recipient <- accountGen.map(_.toAddress)
          amount    <- Gen.choose(100000L, 1000000000L)
        } yield ParsedTransfer(recipient, amount)
        transfers             <- Gen.listOfN(transferCount, transferGen)
        (assetIssue, _, _, _) <- issueReissueBurnGeneratorP(ENOUGH_AMT, master)
        maybeAsset            <- Gen.option(assetIssue)
        transfer              <- massTransferV1GeneratorP(master, transfers, maybeAsset.map(_.id()))
      } yield (genesis, assetIssue, transfer)

      forAll(setup) {
        case (genesis, issue, transfer) =>
          assertDiffAndState(Seq(block(Seq(genesis, issue))), block(Seq(transfer)), fs) {
            case (totalDiff, newState) =>
              assertBalanceInvariant(totalDiff)

              val totalAmount     = transfer.transfers.map(_.amount).sum
              val fees            = issue.fee + transfer.fee
              val senderPortfolio = newState.addressPortfolio(transfer.sender.toAddress)
              transfer.assetId match {
                case Some(aid) => senderPortfolio shouldBe Portfolio(ENOUGH_AMT - fees, LeaseBalance.empty, Map(aid -> (ENOUGH_AMT - totalAmount)))
                case None      => senderPortfolio.balance shouldBe (ENOUGH_AMT - fees - totalAmount)
              }
              for (ParsedTransfer(recipient, amount) <- transfer.transfers) {
                val recipientPortfolio = newState.addressPortfolio(recipient.asInstanceOf[Address])
                if (transfer.sender.toAddress != recipient) {
                  transfer.assetId match {
                    case Some(aid) => recipientPortfolio shouldBe Portfolio(0, LeaseBalance.empty, Map(aid -> amount))
                    case None      => recipientPortfolio shouldBe Portfolio(amount, LeaseBalance.empty, Map.empty)
                  }
                }
              }
          }
      }
    }

    Seq(0, 1, MaxTransferCount) foreach testDiff // test edge cases
    Gen.choose(2, MaxTransferCount - 1) map testDiff
  }

  property("MassTransfer fails on non-existent alias") {
    val setup = for {
      (genesis, master) <- baseSetup
      recipient         <- aliasGen
      amount            <- Gen.choose(100000L, 1000000000L)
      transfer          <- massTransferV1GeneratorP(master, List(ParsedTransfer(recipient, amount)), None)
    } yield (genesis, transfer)

    forAll(setup) {
      case (genesis, transfer) =>
        assertDiffEither(Seq(block(Seq(genesis))), block(Seq(transfer)), fs) { blockDiffEi =>
          blockDiffEi should produce("AliasDoesNotExist")
        }
    }
  }

  property("MassTransfer fails on non-issued asset") {
    val setup = for {
      (genesis, master) <- baseSetup
      recipient         <- accountGen.map(_.toAddress)
      amount            <- Gen.choose(100000L, 1000000000L)
      assetId           <- assetIdGen.filter(_.isDefined)
      transfer          <- massTransferV1GeneratorP(master, List(ParsedTransfer(recipient, amount)), assetId)
    } yield (genesis, transfer)

    forAll(setup) {
      case (genesis, transfer) =>
        assertDiffEither(Seq(block(Seq(genesis))), block(Seq(transfer)), fs) { blockDiffEi =>
          blockDiffEi should produce("Attempt to transfer unavailable funds")
        }
    }
  }

  property("MassTransfer cannot overspend funds") {
    val setup = for {
      (genesis, master)     <- baseSetup
      recipients            <- Gen.listOfN(2, accountGen.map(acc => ParsedTransfer(acc.toAddress, ENOUGH_AMT / 2 + 1)))
      (assetIssue, _, _, _) <- issueReissueBurnGeneratorP(ENOUGH_AMT, master)
      maybeAsset            <- Gen.option(assetIssue)
      transfer              <- massTransferV1GeneratorP(master, recipients, maybeAsset.map(_.id()))
    } yield (genesis, transfer)

    forAll(setup) {
      case (genesis, transfer) =>
        assertDiffEither(Seq(block(Seq(genesis))), block(Seq(transfer)), fs) { blockDiffEi =>
          blockDiffEi should produce("Attempt to transfer unavailable funds")
        }
    }
  }

  property("validation fails prior to feature activation") {
    val setup = for {
      (genesis, master) <- baseSetup
      transfer          <- massTransferV1GeneratorP(master, List(), None)
    } yield (genesis, transfer)
    val settings = TestFunctionalitySettings.Enabled.copy(preActivatedFeatures = Map(BlockchainFeature.MassTransfer.id -> 10))

    forAll(setup) {
      case (genesis, transfer) =>
        assertDiffEither(Seq(block(Seq(genesis))), block(Seq(transfer)), settings) { blockDiffEi =>
          blockDiffEi should produce("has not been activated yet")
        }
    }
  }
}
