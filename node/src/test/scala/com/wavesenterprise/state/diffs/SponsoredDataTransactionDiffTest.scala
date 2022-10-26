package com.wavesenterprise.state.diffs

import com.wavesenterprise.TransactionGen._
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.{FunctionalitySettings, TestFees}
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state.diffs.SponsoredDataTransactionDiffTest._
import com.wavesenterprise.state.{DataEntry, IntegerDataEntry, Sponsorship}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.{AssetId, DataTransaction, DataTransactionV2}
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class SponsoredDataTransactionDiffTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  private val fs = FunctionalitySettings(featureCheckBlocksPeriod = 2,
                                         blocksForFeatureActivation = 1,
                                         preActivatedFeatures = BlockchainFeature.implemented.map(_ -> 0).toMap)

  val setup = for {
    genesisTime                                                              <- ntpTimestampGen.map(_ - 10.minute.toMillis)
    key                                                                      <- validAliasStringGen
    value                                                                    <- positiveLongGen
    (master, masterGenesis)                                                  <- accountGenesisGen(genesisTime)
    (sponsorIssuer, sponsorAssetId, sponsorGenesis, sponsorBlock, startTime) <- issueAndSendSponsorAssetsGen(master, CreateFeeInAsset, genesisTime)
    item         = IntegerDataEntry(key, value)
    dataTx       = data(master, List(item), CreateFeeInAsset, startTime + 1.minute.toMillis, Some(sponsorAssetId))
    genesisBlock = TestBlock.create(Seq(masterGenesis, sponsorGenesis))
    testBlock    = TestBlock.create(master, Seq(dataTx))
  } yield (Seq(genesisBlock, sponsorBlock), testBlock, sponsorIssuer, sponsorAssetId, master.toAddress, dataTx)

  def data(sender: PrivateKeyAccount, data: List[DataEntry[_]], fee: Long, timestamp: Long, feeAssetId: Option[AssetId]): DataTransactionV2 =
    DataTransactionV2.selfSigned(sender, sender, data, timestamp, fee, feeAssetId).explicitGet()

  property("sponsored DataTransactionV2 should proceed") {
    forAll(setup) {
      case (genesisBlock, testBlock, issuer, assetId, master, dataTx) =>
        assertNgDiffState(genesisBlock, testBlock, fs) {
          case (blockDiff, state) =>
            assertBalanceInvariantForSponsorship(blockDiff, master, state.carryFee)
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

object SponsoredDataTransactionDiffTest {
  private val CreateFee           = TestFees.defaultFees.forTxType(DataTransaction.typeId)
  private val CreateFeeInAsset    = Sponsorship.fromWest(CreateFee)
  private val AssetTransferAmount = 100 * CreateFeeInAsset
}
