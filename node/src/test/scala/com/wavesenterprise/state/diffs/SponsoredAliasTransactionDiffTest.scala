package com.wavesenterprise.state.diffs

import com.wavesenterprise.TransactionGen._
import com.wavesenterprise.account.Address
import com.wavesenterprise.block.Block
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.{FunctionalitySettings, TestFees}
import com.wavesenterprise.state._
import com.wavesenterprise.state.diffs.SponsoredAliasTransactionDiffTest._
import com.wavesenterprise.transaction.CreateAliasTransaction
import com.wavesenterprise.utils.NumberUtils.DoubleExt
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class SponsoredAliasTransactionDiffTest extends AnyPropSpec with ScalaCheckPropertyChecks with Matchers with TransactionGen with NoShrink {

  private val fs = FunctionalitySettings(featureCheckBlocksPeriod = 2,
                                         blocksForFeatureActivation = 1,
                                         preActivatedFeatures = BlockchainFeature.implemented.map(_ -> 0).toMap)

  val withCreateV2: Gen[(Seq[Block], Block, Address, ByteStr, Address, CreateAliasTransaction)] = for {
    genesisTime                                                              <- ntpTimestampGen.map(_ - 10.minute.toMillis)
    alias                                                                    <- aliasGen
    (master, masterGenesis)                                                  <- accountGenesisGen(genesisTime)
    (sponsorIssuer, sponsorAssetId, sponsorGenesis, sponsorBlock, startTime) <- issueAndSendSponsorAssetsGen(master, CreateFeeInAsset, genesisTime)
    aliasTx                                                                  <- createAliasV3Gen(master, alias, CreateFeeInAsset, startTime + 1.minute.toMillis, Some(sponsorAssetId))
    genesisBlock = TestBlock.create(Seq(masterGenesis, sponsorGenesis))
    testBlock    = TestBlock.create(master, Seq(aliasTx))
  } yield (Seq(genesisBlock, sponsorBlock), testBlock, sponsorIssuer, sponsorAssetId, master.toAddress, aliasTx)

  property("sponsored CreateAliasTransactionV2 should proceed") {
    forAll(withCreateV2) {
      case (genesisBlock, testBlock, issuer, assetId, master, aliasTx) =>
        assertNgDiffState(genesisBlock, testBlock, fs) {
          case (blockDiff, state) =>
            assertBalanceInvariantForSponsorship(blockDiff, master, state.carryFee)
            aliasTx.feeAssetId shouldBe Some(assetId)
            aliasTx.fee shouldBe CreateFeeInAsset

            blockDiff.portfolios(master).assets(assetId) shouldBe -CreateFeeInAsset
            blockDiff.portfolios(issuer).assets(assetId) shouldBe CreateFeeInAsset
            blockDiff.portfolios(issuer).balance shouldBe -CreateFee

            state.balance(master, Some(assetId)) shouldBe (AssetTransferAmount - CreateFeeInAsset)
            state.balance(issuer, Some(assetId)) shouldBe (ENOUGH_AMT - AssetTransferAmount + CreateFeeInAsset)
            state.balance(issuer, None) shouldBe (ENOUGH_AMT - IssueFee - SponsorshipFee - TransferFee - CreateFee)
        }
    }
  }
}

object SponsoredAliasTransactionDiffTest {
  private val CreateFee           = TestFees.defaultFees.forTxType(CreateAliasTransaction.typeId)
  private val CreateFeeInAsset    = Sponsorship.fromWest(1.west)
  private val AssetTransferAmount = 100 * CreateFeeInAsset
}
