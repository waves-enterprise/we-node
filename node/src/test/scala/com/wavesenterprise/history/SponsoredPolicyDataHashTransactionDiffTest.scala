package com.wavesenterprise.history

import com.wavesenterprise.TransactionGen._
import com.wavesenterprise.acl.OpType
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.history.SponsoredPolicyDataHashTransactionDiffTest._
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.privacy.PolicyDataHash
import com.wavesenterprise.settings.{FunctionalitySettings, TestFees}
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state.Sponsorship
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.transaction._
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class SponsoredPolicyDataHashTransactionDiffTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  private val fs = FunctionalitySettings(featureCheckBlocksPeriod = 2,
                                         blocksForFeatureActivation = 1,
                                         preActivatedFeatures = BlockchainFeature.implemented.map(_ -> 0).toMap)

  private val policyDataHash: PolicyDataHash = PolicyDataHash.fromDataBytes(byteArrayGen(66600).sample.get)

  val setup = for {
    genesisTime                                                              <- ntpTimestampGen.map(_ - 10.minute.toMillis)
    (master, masterGenesis)                                                  <- accountGenesisGen(genesisTime)
    (sponsorIssuer, sponsorAssetId, sponsorGenesis, sponsorBlock, startTime) <- issueAndSendSponsorAssetsGen(master, CreateFeeInAsset, genesisTime)
    createPolicyTx = CreatePolicyTransactionV1
      .selfSigned(
        master,
        "some random policy name",
        "some random policy description",
        List(master.toAddress),
        List(master.toAddress),
        startTime + 1.minute.toMillis,
        CreatePolicyFee
      )
      .explicitGet()
    registerTx = RegisterNodeTransactionV1
      .selfSigned(master, master, Some("node-1"), OpType.Add, startTime + 2.minutes.toMillis, RegisterFee)
      .explicitGet()
    policyDataHashTx = PolicyDataHashTransactionV2
      .selfSigned(master, policyDataHash, createPolicyTx.id(), startTime + 3.minute.toMillis, CreateFeeInAsset, Some(sponsorAssetId))
      .explicitGet()
    genesisBlock       = TestBlock.create(Seq(masterGenesis, sponsorGenesis))
    preconditionsBlock = TestBlock.create(Seq(registerTx, createPolicyTx))
    testBlock          = TestBlock.create(master, Seq(policyDataHashTx))
  } yield (Seq(genesisBlock, sponsorBlock, preconditionsBlock), testBlock, sponsorIssuer, sponsorAssetId, master.toAddress, policyDataHashTx)

  property("sponsored PolicyDataHashTransactionV2 should proceed") {
    forAll(setup) {
      case (genesisBlock, testBlock, issuer, assetId, master, policyDataHashTx) =>
        assertNgDiffState(genesisBlock, testBlock, fs) {
          case (blockDiff, state) =>
            assertBalanceInvariantForSponsorship(blockDiff, master, state.carryFee)
            policyDataHashTx.feeAssetId shouldBe Some(assetId)
            policyDataHashTx.fee shouldBe CreateFeeInAsset

            blockDiff.portfolios(master.toAssetHolder).assets(assetId) shouldBe -CreateFeeInAsset
            blockDiff.portfolios(issuer.toAssetHolder).assets(assetId) shouldBe CreateFeeInAsset
            blockDiff.portfolios(issuer.toAssetHolder).balance shouldBe -CreateFee

            state.addressBalance(master, Some(assetId)) shouldBe (AssetTransferAmount - CreateFeeInAsset)
            state.addressBalance(issuer, Some(assetId)) shouldBe (ENOUGH_AMT - AssetTransferAmount + CreateFeeInAsset)
            state.addressBalance(issuer, None) shouldBe (ENOUGH_AMT - IssueFee - SponsorshipFee - TransferFee - CreateFee)
        }
    }
  }
}

object SponsoredPolicyDataHashTransactionDiffTest {
  private val CreateFee           = TestFees.defaultFees.forTxType(PolicyDataHashTransaction.typeId)
  private val CreatePolicyFee     = TestFees.defaultFees.forTxType(CreatePolicyTransaction.typeId)
  private val RegisterFee         = TestFees.defaultFees.forTxType(RegisterNodeTransactionV1.typeId)
  private val CreateFeeInAsset    = Sponsorship.fromWest(CreateFee)
  private val AssetTransferAmount = 100 * CreateFeeInAsset
}
