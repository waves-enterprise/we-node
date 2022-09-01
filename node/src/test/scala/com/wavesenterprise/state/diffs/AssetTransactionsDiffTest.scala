package com.wavesenterprise.state.diffs

import java.nio.charset.StandardCharsets

import cats._
import com.wavesenterprise.account.AddressScheme
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.lang.ScriptVersion.Versions.V1
import com.wavesenterprise.lang.v1.compiler.CompilerV1
import com.wavesenterprise.lang.v1.parser.Parser
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.settings.TestFunctionalitySettings
import com.wavesenterprise.state._
import com.wavesenterprise.state.diffs.smart.smartEnabledFS
import com.wavesenterprise.transaction.GenesisTransaction
import com.wavesenterprise.transaction.assets._
import com.wavesenterprise.transaction.smart.script.v1.ScriptV1
import com.wavesenterprise.transaction.transfer._
import com.wavesenterprise.transaction.validation.{IssueTransactionValidation, TransferValidation}
import com.wavesenterprise.utils.SmartContractV1Utils.compilerContext
import com.wavesenterprise.{NoShrink, TransactionGen}
import fastparse.Parsed
import org.scalacheck.{Arbitrary, Gen}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class AssetTransactionsDiffTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  def issueReissueBurnTxs(isReissuable: Boolean): Gen[((GenesisTransaction, IssueTransaction), (ReissueTransaction, BurnTransaction))] =
    for {
      master <- accountGen
      ts     <- timestampGen
      genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      ia                     <- positiveLongGen
      ra                     <- positiveLongGen
      ba                     <- positiveLongGen.suchThat(x => x < ia + ra)
      (issue, reissue, burn) <- issueReissueBurnGeneratorP(ia, ra, ba, master) suchThat (_._1.reissuable == isReissuable)
    } yield ((genesis, issue), (reissue, burn))

  property("Issue+Reissue+Burn do not break WEST invariant and updates state") {
    forAll(issueReissueBurnTxs(isReissuable = true)) {
      case (((gen, issue), (reissue, burn))) =>
        assertDiffAndState(Seq(TestBlock.create(Seq(gen, issue))), TestBlock.create(Seq(reissue, burn))) {
          case (blockDiff, newState) =>
            val totalPortfolioDiff = Monoid.combineAll(blockDiff.portfolios.values)

            totalPortfolioDiff.balance shouldBe 0
            totalPortfolioDiff.effectiveBalance shouldBe 0
            totalPortfolioDiff.assets shouldBe Map(reissue.assetId -> (reissue.quantity - burn.amount))

            val totalAssetVolume = issue.quantity + reissue.quantity - burn.amount
            newState.portfolio(issue.sender.toAddress).assets shouldBe Map(reissue.assetId -> totalAssetVolume)
        }
    }
  }

  property("Cannot reissue/burn non-existing alias") {
    val setup: Gen[(GenesisTransaction, ReissueTransaction, BurnTransaction)] = for {
      master <- accountGen
      ts     <- timestampGen
      genesis: GenesisTransaction = GenesisTransaction.create(master.toAddress, ENOUGH_AMT, ts).explicitGet()
      reissue <- reissueGen
      burn    <- burnGen
    } yield (genesis, reissue, burn)

    forAll(setup) {
      case ((gen, reissue, burn)) =>
        assertDiffEi(Seq(TestBlock.create(Seq(gen))), TestBlock.create(Seq(reissue))) { blockDiffEi =>
          blockDiffEi should produce("Referenced assetId not found")
        }
        assertDiffEi(Seq(TestBlock.create(Seq(gen))), TestBlock.create(Seq(burn))) { blockDiffEi =>
          blockDiffEi should produce("Referenced assetId not found")
        }
    }
  }

  property("Cannot reissue/burn non-owned alias") {
    val setup = for {
      ((gen, issue), (_, _)) <- issueReissueBurnTxs(isReissuable = true)
      other                  <- accountGen.suchThat(_ != issue.sender.toAddress)
      quantity               <- positiveLongGen
      reissuable2            <- Arbitrary.arbitrary[Boolean]
      fee                    <- Gen.choose(1L, 2000000L)
      timestamp              <- timestampGen
      reissue = ReissueTransactionV2
        .selfSigned(currentChainId, other, issue.assetId(), quantity, reissuable2, fee, timestamp)
        .explicitGet()
      burn = BurnTransactionV2.selfSigned(currentChainId, other, issue.assetId(), quantity, fee, timestamp).explicitGet()
    } yield ((gen, issue), reissue, burn)

    forAll(setup) {
      case ((gen, issue), reissue, burn) =>
        assertDiffEi(Seq(TestBlock.create(Seq(gen, issue))), TestBlock.create(Seq(reissue))) { blockDiffEi =>
          blockDiffEi should produce("Asset was issued by other address")
        }
        assertDiffEi(Seq(TestBlock.create(Seq(gen, issue))), TestBlock.create(Seq(burn))) { blockDiffEi =>
          blockDiffEi should produce("Asset was issued by other address")
        }
    }
  }

  property("Can burn non-owned alias if feature 'BurnAnyTokens' activated") {
    val setup = for {
      issuer    <- accountGen
      burner    <- accountGen.suchThat(_ != issuer)
      timestamp <- timestampGen
      genesis: GenesisTransaction = GenesisTransaction.create(issuer.toAddress, ENOUGH_AMT, timestamp).right.get
      (issue, _, _) <- issueReissueBurnGeneratorP(ENOUGH_AMT, issuer)
      assetTransfer <- transferGeneratorP(issuer, burner.toAddress, Some(issue.assetId()), None)
      westTransfer  <- westTransferGeneratorP(issuer, burner.toAddress)
      burn = BurnTransactionV2
        .selfSigned(currentChainId, burner, issue.assetId(), assetTransfer.amount, westTransfer.amount, timestamp)
        .right
        .get
    } yield (genesis, issue, assetTransfer, westTransfer, burn)

    val fs =
      TestFunctionalitySettings.Enabled
        .copy(
          preActivatedFeatures = Map(BlockchainFeature.SmartAccounts.id -> 0, BlockchainFeature.BurnAnyTokens.id -> 0)
        )

    forAll(setup) {
      case (genesis, issue, assetTransfer, westTransfer, burn) =>
        assertDiffAndState(Seq(TestBlock.create(Seq(genesis, issue, assetTransfer, westTransfer))), TestBlock.create(Seq(burn)), fs) {
          case (_, newState) =>
            newState.portfolio(burn.sender.toAddress).assets shouldBe Map(burn.assetId -> 0)
        }
    }
  }

  property("Can not reissue > long.max") {
    val setup = for {
      issuer    <- accountGen
      timestamp <- timestampGen
      genesis: GenesisTransaction = GenesisTransaction.create(issuer.toAddress, ENOUGH_AMT, timestamp).explicitGet()
      assetName   <- genBoundedString(IssueTransactionValidation.MinAssetNameLength, IssueTransactionValidation.MaxAssetNameLength)
      description <- genBoundedString(0, IssueTransactionValidation.MaxDescriptionLength)
      quantity    <- Gen.choose(Long.MaxValue / 200, Long.MaxValue / 100)
      fee         <- Gen.choose(MinIssueFee, 2 * MinIssueFee)
      decimals    <- Gen.choose(1: Byte, 8: Byte)
      issue       <- createIssue(issuer, assetName, description, quantity, decimals, true, fee, timestamp)
      assetId = issue.assetId()
      reissue = ReissueTransactionV2.selfSigned(currentChainId, issuer, assetId, Long.MaxValue, true, 1, timestamp).explicitGet()
    } yield (issuer, assetId, genesis, issue, reissue)

    val fs =
      TestFunctionalitySettings.Enabled
        .copy(
          preActivatedFeatures = Map(BlockchainFeature.SmartAccounts.id -> 0, BlockchainFeature.DataTransaction.id -> 0)
        )

    forAll(setup) {
      case (issuer, assetId, genesis, issue, reissue) =>
        assertDiffEi(Seq(TestBlock.create(Seq(genesis, issue))), TestBlock.create(Seq(reissue)), fs) { ei =>
          ei should produce("Asset total value overflow")
        }
    }
  }

  property("Can request reissue > long.max before BurnAnyTokens activated") {
    val setup = for {
      issuer    <- accountGen
      timestamp <- timestampGen
      genesis: GenesisTransaction = GenesisTransaction.create(issuer.toAddress, ENOUGH_AMT, timestamp).explicitGet()
      assetName   <- genBoundedString(IssueTransactionValidation.MinAssetNameLength, IssueTransactionValidation.MaxAssetNameLength)
      description <- genBoundedString(0, IssueTransactionValidation.MaxDescriptionLength)
      quantity    <- Gen.choose(Long.MaxValue / 200, Long.MaxValue / 100)
      fee         <- Gen.choose(MinIssueFee, 2 * MinIssueFee)
      decimals    <- Gen.choose(1: Byte, 8: Byte)
      issue       <- createIssue(issuer, assetName, description, quantity, decimals, true, fee, timestamp)
      assetId = issue.assetId()
      reissue = ReissueTransactionV2.selfSigned(currentChainId, issuer, assetId, Long.MaxValue, true, 1, timestamp).explicitGet()
    } yield (issuer, assetId, genesis, issue, reissue)

    val fs =
      TestFunctionalitySettings.Enabled

    forAll(setup) {
      case (issuer, assetId, genesis, issue, reissue) =>
        assertDiffEi(Seq(TestBlock.create(Seq(genesis, issue))), TestBlock.create(Seq(reissue)), fs) { ei =>
          ei should produce("negative asset balance")
        }
    }
  }

  property("Can not total issue > long.max") {
    val setup = for {
      issuer    <- accountGen
      holder    <- accountGen.suchThat(_ != issuer)
      timestamp <- timestampGen
      genesis: GenesisTransaction = GenesisTransaction.create(issuer.toAddress, ENOUGH_AMT, timestamp).explicitGet()
      assetName   <- genBoundedString(IssueTransactionValidation.MinAssetNameLength, IssueTransactionValidation.MaxAssetNameLength)
      description <- genBoundedString(0, IssueTransactionValidation.MaxDescriptionLength)
      quantity    <- Gen.choose(Long.MaxValue / 200, Long.MaxValue / 100)
      fee         <- Gen.choose(MinIssueFee, 2 * MinIssueFee)
      decimals    <- Gen.choose(1: Byte, 8: Byte)
      issue       <- createIssue(issuer, assetName, description, quantity, decimals, true, fee, timestamp)
      assetId = issue.assetId()
      attachment <- genBoundedBytes(0, TransferValidation.MaxAttachmentSize)
      transfer = TransferTransactionV2
        .selfSigned(issuer, Some(assetId), None, timestamp, quantity - 1, fee, holder.toAddress, attachment)
        .explicitGet()
      reissue = ReissueTransactionV2
        .selfSigned(currentChainId, issuer, assetId, (Long.MaxValue - quantity) + 1, true, 1, timestamp)
        .explicitGet()
    } yield (issuer, assetId, genesis, issue, reissue, transfer)

    val fs =
      TestFunctionalitySettings.Enabled
        .copy(
          preActivatedFeatures = Map(BlockchainFeature.SmartAccounts.id -> 0, BlockchainFeature.DataTransaction.id -> 0)
        )

    forAll(setup) {
      case (issuer, assetId, genesis, issue, reissue, transfer) =>
        assertDiffEi(Seq(TestBlock.create(Seq(genesis, issue, transfer))), TestBlock.create(Seq(reissue)), fs) { ei =>
          ei should produce("Asset total value overflow")
        }
    }
  }

  property("Cannot reissue non-reissuable alias") {
    forAll(issueReissueBurnTxs(isReissuable = false)) {
      case ((gen, issue), (reissue, _)) =>
        assertDiffEi(Seq(TestBlock.create(Seq(gen, issue))), TestBlock.create(Seq(reissue))) { blockDiffEi =>
          blockDiffEi should produce("Asset is not reissuable")
        }
    }
  }

  private def createScript(code: String) = {
    val Parsed.Success(expr, _) = Parser(code).get
    ScriptV1(CompilerV1(compilerContext(V1, isAssetScript = false), expr).explicitGet()._1).explicitGet()
  }

  def genesisIssueTransferReissue(code: String): Gen[(Seq[GenesisTransaction], IssueTransaction, TransferTransaction, ReissueTransaction)] =
    for {
      timestamp         <- timestampGen
      initialWestAmount <- Gen.choose(Long.MaxValue / 1000, Long.MaxValue / 100)
      accountA          <- accountGen
      accountB          <- accountGen
      smallFee          <- Gen.choose(1l, 10l)
      genesisTx1 = GenesisTransaction.create(accountA.toAddress, initialWestAmount, timestamp).explicitGet()
      genesisTx2 = GenesisTransaction.create(accountB.toAddress, initialWestAmount, timestamp).explicitGet()
      reissuable = true
      (_, assetName, description, quantity, decimals, _, _, _) <- issueParamGen
      issue = IssueTransactionV2
        .selfSigned(AddressScheme.getAddressSchema.chainId,
                    accountA,
                    assetName,
                    description,
                    quantity,
                    decimals,
                    reissuable,
                    smallFee,
                    timestamp + 1,
                    Some(createScript(code)))
        .explicitGet()
      assetId = issue.id()
      transfer = TransferTransactionV2
        .selfSigned(accountA, Some(assetId), None, timestamp + 2, issue.quantity, smallFee, accountB.toAddress, Array.empty)
        .explicitGet()
      reissue = ReissueTransactionV2
        .selfSigned(currentChainId, accountB, assetId, quantity, reissuable, smallFee, timestamp + 3)
        .explicitGet()
    } yield (Seq(genesisTx1, genesisTx2), issue, transfer, reissue)

  property("Can issue smart asset with script") {
    forAll(for {
      acc        <- accountGen
      genesis    <- genesisGeneratorP(acc.toAddress)
      smartIssue <- smartIssueTransactionGen(acc)
    } yield (genesis, smartIssue)) {
      case (gen, issue) =>
        assertDiffAndState(Seq(TestBlock.create(Seq(gen))), TestBlock.create(Seq(issue)), smartEnabledFS) {
          case (blockDiff, newState) =>
            newState.assetDescription(issue.id()) shouldBe Some(
              AssetDescription(
                issue.sender,
                2,
                issue.timestamp,
                new String(issue.name, StandardCharsets.UTF_8),
                new String(issue.description, StandardCharsets.UTF_8),
                issue.decimals,
                issue.reissuable,
                BigInt(issue.quantity),
                issue.script,
                sponsorshipIsEnabled = false
              ))
            blockDiff.transactionsMap.contains(issue.id()) shouldBe true
            newState.transactionInfo(issue.id()).isDefined shouldBe true
            newState.transactionInfo(issue.id()).isDefined shouldEqual true
        }
    }
  }

  property("Can transfer when script evaluates to TRUE") {
    forAll(genesisIssueTransferReissue("true")) {
      case (gen, issue, transfer, _) =>
        assertDiffAndState(Seq(TestBlock.create(gen)), TestBlock.create(Seq(issue, transfer)), smartEnabledFS) {
          case (blockDiff, newState) =>
            val totalPortfolioDiff = Monoid.combineAll(blockDiff.portfolios.values)
            totalPortfolioDiff.assets(issue.id()) shouldEqual issue.quantity
            newState.balance(newState.resolveAlias(transfer.recipient).explicitGet(), Some(issue.id())) shouldEqual transfer.amount
        }
    }
  }

  property("Cannot transfer when script evaluates to FALSE") {
    forAll(genesisIssueTransferReissue("false")) {
      case (gen, issue, transfer, _) =>
        assertDiffEi(Seq(TestBlock.create(gen)), TestBlock.create(Seq(issue, transfer)), smartEnabledFS)(ei =>
          ei should produce("TransactionNotAllowedByScript"))
    }
  }

  property("Cannot reissue when script evaluates to FALSE") {
    forAll(genesisIssueTransferReissue("false")) {
      case (gen, issue, _, reissue) =>
        assertDiffEi(Seq(TestBlock.create(gen)), TestBlock.create(Seq(issue, reissue)), smartEnabledFS)(ei =>
          ei should produce("TransactionNotAllowedByScript"))
    }
  }

  property("Only issuer can reissue") {
    forAll(genesisIssueTransferReissue("true")) {
      case (gen, issue, _, reissue) =>
        assertDiffEi(Seq(TestBlock.create(gen)), TestBlock.create(Seq(issue, reissue)), smartEnabledFS) { ei =>
          ei should produce("Asset was issued by other address")
        }
    }
  }

}
