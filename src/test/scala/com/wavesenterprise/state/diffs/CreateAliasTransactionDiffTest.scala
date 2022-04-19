package com.wavesenterprise.state.diffs

import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.TestFunctionalitySettings
import com.wavesenterprise.state._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.assets.IssueTransaction
import com.wavesenterprise.transaction.{CreateAliasTransaction, CreateAliasTransactionV2, GenesisTransaction}
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{Matchers, PropSpec}

class CreateAliasTransactionDiffTest extends PropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  val fs =
    TestFunctionalitySettings.Enabled.copy(
      preActivatedFeatures = Map(BlockchainFeature.SmartAccounts.id -> 0, BlockchainFeature.SponsoredFeesSupport.id -> 0)
    )

  val preconditionsAndAliasCreations
    : Gen[(GenesisTransaction, CreateAliasTransaction, CreateAliasTransaction, CreateAliasTransaction, CreateAliasTransaction)] = for {
    master <- accountGen
    ts     <- timestampGen
    genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
    alias                    <- aliasGen
    alias2                   <- aliasGen suchThat (_.name != alias.name)
    fee                      <- smallFeeGen
    other: PrivateKeyAccount <- accountGen
    aliasTx                  <- createAliasV2Gen(master, alias, fee, ts)
    sameAliasTx              <- createAliasV2Gen(master, alias, fee + 1, ts)
    sameAliasOtherSenderTx   <- createAliasV2Gen(other, alias, fee + 2, ts)
    anotherAliasTx           <- createAliasV2Gen(master, alias2, fee + 3, ts)
  } yield (genesis, aliasTx, sameAliasTx, sameAliasOtherSenderTx, anotherAliasTx)

  property("cannot create more than one aliases for address") {
    forAll(preconditionsAndAliasCreations) {
      case (gen, aliasTx, _, _, anotherAliasTx) =>
        assertDiffEi(Seq(TestBlock.create(Seq(gen, aliasTx))), TestBlock.create(Seq(anotherAliasTx)), fs) { blockDiffEi =>
          blockDiffEi should produce("Only one alias per address is allowed")
        }
    }
  }

  property("cannot recreate existing alias") {
    forAll(preconditionsAndAliasCreations) {
      case (gen, aliasTx, sameAliasTx, sameAliasOtherSenderTx, _) =>
        assertDiffEi(Seq(TestBlock.create(Seq(gen, aliasTx))), TestBlock.create(Seq(sameAliasTx)), fs) { blockDiffEi =>
          blockDiffEi should produce("AlreadyInTheState")
        }

        assertDiffEi(Seq(TestBlock.create(Seq(gen, aliasTx))), TestBlock.create(Seq(sameAliasOtherSenderTx)), fs) { blockDiffEi =>
          blockDiffEi should produce("AlreadyInTheState")
        }
    }
  }

  val preconditionsTransferLease = for {
    master           <- accountGen
    aliasedRecipient <- otherAccountGen(candidate = master)
    ts               <- positiveIntGen
    gen: GenesisTransaction  = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
    gen2: GenesisTransaction = GenesisTransaction.create(aliasedRecipient.toAddress, ENOUGH_AMT + 1, ts).explicitGet()
    issue1: IssueTransaction <- issueReissueBurnGeneratorP(ENOUGH_AMT, master).map(_._1)
    issue2: IssueTransaction <- issueReissueBurnGeneratorP(ENOUGH_AMT, master).map(_._1)
    maybeAsset               <- Gen.option(issue1)
    maybeAsset2              <- Gen.option(issue2)
    maybeFeeAsset            <- Gen.oneOf(maybeAsset, maybeAsset2)
    alias                    <- aliasGen
    fee                      <- smallFeeGen
    aliasTx = CreateAliasTransactionV2.selfSigned(aliasedRecipient, alias, fee, ts).explicitGet()
    transfer <- transferGeneratorP(master, alias, maybeAsset.map(_.id()), maybeFeeAsset.map(_.id()))
    lease    <- leaseAndCancelGeneratorP(master, alias, master).map(_._1)
  } yield (gen, gen2, issue1, issue2, aliasTx, transfer, lease)

  property("Can transfer to alias") {
    forAll(preconditionsTransferLease) {
      case (gen, gen2, issue1, issue2, aliasTx, transfer, _) =>
        assertDiffAndState(Seq(TestBlock.create(Seq(gen, gen2, issue1, issue2, aliasTx))), TestBlock.create(Seq(transfer)), fs) {
          case (blockDiff, _) =>
            if (transfer.sender.toAddress != aliasTx.sender.toAddress) {
              val recipientPortfolioDiff = blockDiff.portfolios(aliasTx.sender.toAddress)
              transfer.assetId match {
                case Some(aid) => recipientPortfolioDiff shouldBe Portfolio(0, LeaseBalance.empty, Map(aid -> transfer.amount))
                case None      => recipientPortfolioDiff shouldBe Portfolio(transfer.amount, LeaseBalance.empty, Map.empty)
              }
            }
        }
    }
  }

  property("Can lease to alias except for self") {
    forAll(preconditionsTransferLease) {
      case (gen, gen2, issue1, issue2, aliasTx, _, lease) =>
        assertDiffEi(Seq(TestBlock.create(Seq(gen, gen2, issue1, issue2, aliasTx))), TestBlock.create(Seq(lease)), fs) { blockDiffEi =>
          if (lease.sender.toAddress != aliasTx.sender.toAddress) {
            val recipientPortfolioDiff = blockDiffEi.explicitGet().portfolios(aliasTx.sender.toAddress)
            recipientPortfolioDiff shouldBe Portfolio(0, LeaseBalance(lease.amount, 0), Map.empty)
          } else {
            blockDiffEi should produce("Cannot lease to self")
          }
        }
    }
  }
}
