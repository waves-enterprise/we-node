package com.wavesenterprise.history

import com.wavesenterprise.TransactionGen._
import com.wavesenterprise.acl.OpType
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.history.SponsoredPolicyTransactionDiffTest._
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.{FunctionalitySettings, TestFees}
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state.diffs.{ENOUGH_AMT, _}
import com.wavesenterprise.state._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction._
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class SponsoredPolicyTransactionDiffTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  private val fs = FunctionalitySettings(featureCheckBlocksPeriod = 2,
                                         blocksForFeatureActivation = 1,
                                         preActivatedFeatures = BlockchainFeature.implemented.map(_ -> 0).toMap)

  val setup = for {
    genesisTime                                                              <- ntpTimestampGen.map(_ - 10.minute.toMillis)
    (master, masterGenesis)                                                  <- accountGenesisGen(genesisTime)
    (sponsorIssuer, sponsorAssetId, sponsorGenesis, sponsorBlock, startTime) <- issueAndSendSponsorAssetsGen(master, CreateFeeInAsset, genesisTime)
    createPolicyTx = CreatePolicyTransactionV2
      .selfSigned(
        master,
        "some random policy name",
        "some random policy description",
        List(master.toAddress),
        List(master.toAddress),
        startTime + 1.minute.toMillis,
        CreateFee,
        Some(sponsorAssetId)
      )
      .explicitGet()
    someOwner <- accountGen
    updatePolicyTx = UpdatePolicyTransactionV2
      .selfSigned(
        master,
        createPolicyTx.id(),
        List(someOwner.toAddress),
        List(someOwner.toAddress),
        OpType.Add,
        startTime + 2.minutes.toMillis,
        UpdateFee,
        Some(sponsorAssetId)
      )
      .explicitGet()
    registerTx = RegisterNodeTransactionV1
      .selfSigned(master, master, Some("node-1"), OpType.Add, startTime + 3.minutes.toMillis, RegisterFee)
      .explicitGet()
    registerTx2 = RegisterNodeTransactionV1
      .selfSigned(master, someOwner, Some("node-2"), OpType.Add, startTime + 4.minutes.toMillis, RegisterFee)
      .explicitGet()
    genesisAcc         = GenesisTransaction.create(someOwner.toAddress, ENOUGH_AMT, genesisTime).explicitGet()
    genesisBlock       = TestBlock.create(Seq(masterGenesis, sponsorGenesis, genesisAcc))
    preconditionsBlock = TestBlock.create(Seq(registerTx, registerTx2))
    createBlock        = TestBlock.create(master, Seq(createPolicyTx))
    updateBlock        = TestBlock.create(master, Seq(updatePolicyTx))
  } yield (Seq(genesisBlock, sponsorBlock, preconditionsBlock), createBlock, updateBlock, sponsorIssuer, sponsorAssetId, master.toAddress, createPolicyTx)

  property("sponsored CreatePolicyTransactionV2 should proceed") {
    forAll(setup) {
      case (genesisBlock, createBlock, _, issuer, assetId, master, createPolicyTx) =>
        assertNgDiffState(genesisBlock, createBlock, fs) {
          case (blockDiff, state) =>
            assertBalanceInvariantForSponsorship(blockDiff, master, state.carryFee)
            createPolicyTx.feeAssetId shouldBe Some(assetId)
            createPolicyTx.fee shouldBe CreateFeeInAsset

            blockDiff.portfolios(master.toAssetHolder).assets(assetId) shouldBe -CreateFeeInAsset
            blockDiff.portfolios(issuer.toAssetHolder).assets(assetId) shouldBe CreateFeeInAsset
            blockDiff.portfolios(issuer.toAssetHolder).balance shouldBe -CreateFee

            state.addressBalance(master, Some(assetId)) shouldBe (AssetTransferAmount - CreateFeeInAsset)
            state.addressBalance(issuer, Some(assetId)) shouldBe (ENOUGH_AMT - AssetTransferAmount + CreateFeeInAsset)
            state.addressBalance(issuer, None) shouldBe (ENOUGH_AMT - IssueFee - SponsorshipFee - TransferFee - CreateFee)
        }
    }
  }

  property("sponsored UpdatePolicyTransactionV2 should proceed") {
    forAll(setup) {
      case (genesisBlock, createBlock, updateBlock, issuer, assetId, master, createPolicyTx) =>
        assertNgDiffState(genesisBlock :+ createBlock, updateBlock, fs) {
          case (blockDiff, state) =>
            assertBalanceInvariantForSponsorship(blockDiff, master, state.carryFee)
            createPolicyTx.feeAssetId shouldBe Some(assetId)
            createPolicyTx.fee shouldBe CreateFeeInAsset

            blockDiff.portfolios(master.toAssetHolder).assets(assetId) shouldBe -(CreateFeeInAsset - UpdateFeeInAsset)
            blockDiff.portfolios(issuer.toAssetHolder).assets(assetId) shouldBe CreateFeeInAsset - UpdateFeeInAsset
            blockDiff.portfolios(issuer.toAssetHolder).balance shouldBe -(CreateFee - UpdateFee)

            state.addressBalance(master, Some(assetId)) shouldBe (AssetTransferAmount - CreateFeeInAsset - UpdateFeeInAsset)
            state.addressBalance(issuer, Some(assetId)) shouldBe (ENOUGH_AMT - AssetTransferAmount + CreateFeeInAsset + UpdateFeeInAsset)
            state.addressBalance(issuer, None) shouldBe (ENOUGH_AMT - IssueFee - SponsorshipFee - TransferFee - CreateFee - UpdateFee)
        }
    }
  }

}

object SponsoredPolicyTransactionDiffTest {
  private val CreateFee           = TestFees.defaultFees.forTxType(CreatePolicyTransaction.typeId)
  private val UpdateFee           = TestFees.defaultFees.forTxType(UpdatePolicyTransaction.typeId)
  private val RegisterFee         = TestFees.defaultFees.forTxType(RegisterNodeTransactionV1.typeId)
  private val CreateFeeInAsset    = Sponsorship.fromWest(CreateFee)
  private val UpdateFeeInAsset    = Sponsorship.fromWest(UpdateFee)
  private val AssetTransferAmount = 100 * CreateFeeInAsset
}
