package com.wavesenterprise.state.diffs

import com.wavesenterprise.TransactionGen._
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.{FunctionalitySettings, TestFees}
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state.Sponsorship
import com.wavesenterprise.state.diffs.SponsoredMassTransferTransactionDiffTest._
import com.wavesenterprise.transaction.transfer.{MassTransferTransaction, MassTransferTransactionV2, ParsedTransfer}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._

class SponsoredMassTransferTransactionDiffTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  private val fs = FunctionalitySettings(featureCheckBlocksPeriod = 2,
                                         blocksForFeatureActivation = 1,
                                         preActivatedFeatures = BlockchainFeature.implemented.map(_ -> 0).toMap)

  val setup = for {
    genesisTime                                                              <- ntpTimestampGen.map(_ - 10.minute.toMillis)
    (master, masterGenesis)                                                  <- accountGenesisGen(genesisTime)
    (sponsorIssuer, sponsorAssetId, sponsorGenesis, sponsorBlock, startTime) <- issueAndSendSponsorAssetsGen(master, CreateFeeInAsset, genesisTime)
    recipient                                                                <- accountGen.map(_.toAddress)
    amount    = AdditionalMassTransferFee
    transfers = ParsedTransfer(recipient, amount)
    transferTx = MassTransferTransactionV2
      .selfSigned(master, None, List(transfers), startTime + 1.minute.toMillis, CreateFeeInAsset, Array.empty[Byte], Some(sponsorAssetId))
      .explicitGet()
    genesisBlock = TestBlock.create(Seq(masterGenesis, sponsorGenesis))
    testBlock    = TestBlock.create(master, Seq(transferTx))
  } yield (Seq(genesisBlock, sponsorBlock), testBlock, sponsorIssuer, sponsorAssetId, master.toAddress, transferTx)

  property("sponsored MassTransferTransactionV2 should proceed") {
    forAll(setup) {
      case (genesisBlock, testBlock, issuer, assetId, master, dataTx) =>
        assertNgDiffState(genesisBlock, testBlock, fs) {
          case (blockDiff, state) =>
            assertBalanceInvariantForSponsorship(blockDiff, master, state.carryFee, AdditionalMassTransferFee)
            dataTx.feeAssetId shouldBe Some(assetId)
            dataTx.fee shouldBe CreateFeeInAsset

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

object SponsoredMassTransferTransactionDiffTest {
  private val CreateFee                 = 10 * TestFees.defaultFees.forTxType(MassTransferTransaction.typeId)
  private val AdditionalMassTransferFee = TestFees.defaultFees.forTxType(MassTransferTransaction.typeId)
  private val CreateFeeInAsset          = Sponsorship.fromWest(CreateFee)
  private val AssetTransferAmount       = 100 * CreateFeeInAsset
}
