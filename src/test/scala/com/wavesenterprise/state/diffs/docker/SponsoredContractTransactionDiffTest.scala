package com.wavesenterprise.state.diffs.docker

import com.wavesenterprise.{NoShrink, TransactionGen}
import com.wavesenterprise.TransactionGen._
import com.wavesenterprise.account.{Address, PrivateKeyAccount}
import com.wavesenterprise.block.Block
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.FunctionalitySettings
import com.wavesenterprise.settings.TestFees.{defaultFees => fees}
import com.wavesenterprise.state.diffs.{ENOUGH_AMT, assertBalanceInvariantForSponsorship, assertNgDiffState}
import com.wavesenterprise.state.{ByteStr, DataEntry, Sponsorship}
import com.wavesenterprise.transaction.docker._
import org.scalacheck.Gen
import org.scalatest.{CancelAfterFailure, Matchers, PropSpec}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._

class SponsoredContractTransactionDiffTest
    extends PropSpec
    with ScalaCheckPropertyChecks
    with Matchers
    with CancelAfterFailure
    with ContractTransactionGen
    with TransactionGen
    with NoShrink {

  import SponsoredContractTransactionDiffTest._

  private val fs = FunctionalitySettings(
    featureCheckBlocksPeriod = 2,
    blocksForFeatureActivation = 1,
    preActivatedFeatures = BlockchainFeature.implemented.filterNot(_ == BlockchainFeature.ContractValidationsSupport.id).map(_ -> 0).toMap
  )

  implicit class ListExt(list: List[DataEntry[_]]) {
    def asMap: Map[String, DataEntry[_]] = list.map(e => e.key -> e).toMap
  }

  val withCreateV2: Gen[(Seq[Block], Address, ByteStr, PrivateKeyAccount, PrivateKeyAccount, ExecutedContractTransactionV1)] = for {
    genesisTime                                        <- ntpTimestampGen.map(_ - 10.minute.toMillis)
    (contractDeveloper, genesisForCreateAccount)       <- accountGenesisGen(genesisTime)
    (issuer, assetId, sponsorGenesis, sponsorBlock, _) <- issueAndSendSponsorAssetsGen(contractDeveloper, CreateFeeInAsset, genesisTime)
    create                                             <- createContractV2ParamGen((Gen.const(Some(assetId)), Gen.const(CreateFeeInAsset)), Gen.const(contractDeveloper))
    executedSigner                                     <- accountGen
    executedCreate                                     <- executedContractV1ParamGen(executedSigner, create)
    genesisBlock = TestBlock.create(Seq(sponsorGenesis, genesisForCreateAccount))
  } yield (Seq(genesisBlock, sponsorBlock), issuer, assetId, contractDeveloper, executedSigner, executedCreate)

  val withCallV3: Gen[(Seq[Block], Address, ByteStr, PrivateKeyAccount, ExecutedContractTransactionV1)] = for {
    (blocks, issuer, assetId, contractDeveloper, executedSigner, executedCreate) <- withCreateV2
    executedCreateBlock = TestBlock.create(executedSigner, Seq(executedCreate))
    call         <- callContractV3ParamGen((Gen.const(Some(assetId)), Gen.const(CallFeeInAsset)), contractDeveloper, executedCreate.tx.contractId, 1)
    executedCall <- executedContractV1ParamGen(executedSigner, call)
  } yield (blocks :+ executedCreateBlock, issuer, assetId, executedSigner, executedCall)

  val withUpdateV2: Gen[(Seq[Block], Address, ByteStr, PrivateKeyAccount, ExecutedContractTransactionV1)] = for {
    (blocks, issuer, assetId, contractDeveloper, executedSigner, executedCreate) <- withCreateV2
    executedCreateBlock = TestBlock.create(executedSigner, Seq(executedCreate))
    update         <- updateContractV2ParamGen((Gen.const(Some(assetId)), Gen.const(UpdateFeeInAsset)), contractDeveloper, executedCreate.tx.contractId)
    executedUpdate <- executedContractV1ParamGen(executedSigner, update)
  } yield (blocks :+ executedCreateBlock, issuer, assetId, executedSigner, executedUpdate)

  val withDisableV2: Gen[(Seq[Block], Address, ByteStr, PrivateKeyAccount, DisableContractTransactionV2)] = for {
    (blocks, issuer, assetId, contractDeveloper, executedSigner, executedCreate) <- withCreateV2
    executedCreateBlock = TestBlock.create(executedSigner, Seq(executedCreate))
    disable <- disableContractV2ParamGen((Gen.const(Some(assetId)), Gen.const(DisableFeeInAsset)), contractDeveloper, executedCreate.tx.contractId)
  } yield (blocks :+ executedCreateBlock, issuer, assetId, executedSigner, disable)

  property("sponsored CreateContractTransactionV2 should proceed") {
    forAll(withCreateV2) {
      case (blocks, issuer, assetId, _, executedSigner, executedCreate) =>
        assertNgDiffState(blocks, TestBlock.create(executedSigner, Seq(executedCreate)), fs) {
          case (blockDiff, state) =>
            assertBalanceInvariantForSponsorship(blockDiff, executedSigner.toAddress, state.carryFee)

            val create       = executedCreate.tx
            val createSender = create.sender.toAddress
            create.feeAssetId shouldBe Some(assetId)
            create.fee shouldBe CreateFeeInAsset

            blockDiff.portfolios(createSender).assets(assetId) shouldBe -CreateFeeInAsset
            blockDiff.portfolios(issuer).assets(assetId) shouldBe CreateFeeInAsset
            blockDiff.portfolios(issuer).balance shouldBe -CreateFee

            state.balance(createSender, Some(assetId)) shouldBe (AssetTransferAmount - CreateFeeInAsset)
            state.balance(issuer, Some(assetId)) shouldBe (ENOUGH_AMT - AssetTransferAmount + CreateFeeInAsset)
            state.balance(issuer, None) shouldBe (ENOUGH_AMT - IssueFee - SponsorshipFee - TransferFee - CreateFee)
        }
    }
  }

  property("sponsored CallContractTransactionV3 should proceed") {
    forAll(withCallV3) {
      case (blocks, issuer, assetId, executedSigner, executedCall) =>
        assertNgDiffState(blocks, TestBlock.create(executedSigner, Seq(executedCall)), fs) {
          case (blockDiff, state) =>
            assertBalanceInvariantForSponsorship(blockDiff, executedSigner.toAddress, state.carryFee)

            val call       = executedCall.tx
            val callSender = call.sender.toAddress
            call.feeAssetId shouldBe Some(assetId)
            call.fee shouldBe CallFeeInAsset

            blockDiff.portfolios(callSender).assets(assetId) shouldBe -CallFeeInAsset
            blockDiff.portfolios(issuer).assets(assetId) shouldBe CallFeeInAsset
            blockDiff.portfolios(issuer).balance shouldBe -CallFee
        }
    }
  }

  property("sponsored UpdateContractTransactionV2 should proceed") {
    forAll(withUpdateV2) {
      case (blocks, issuer, assetId, executedSigner, executedUpdate) =>
        assertNgDiffState(blocks, TestBlock.create(executedSigner, Seq(executedUpdate)), fs) {
          case (blockDiff, state) =>
            assertBalanceInvariantForSponsorship(blockDiff, executedSigner.toAddress, state.carryFee)

            val update       = executedUpdate.tx
            val updateSender = update.sender.toAddress
            update.feeAssetId shouldBe Some(assetId)
            update.fee shouldBe UpdateFeeInAsset

            blockDiff.portfolios(updateSender).assets(assetId) shouldBe -UpdateFeeInAsset
            blockDiff.portfolios(issuer).assets(assetId) shouldBe UpdateFeeInAsset
            blockDiff.portfolios(issuer).balance shouldBe -UpdateFee
        }
    }
  }

  property("sponsored DisableContractTransactionV2 should proceed") {
    forAll(withDisableV2) {
      case (blocks, issuer, assetId, executedSigner, disable) =>
        assertNgDiffState(blocks, TestBlock.create(executedSigner, Seq(disable)), fs) {
          case (blockDiff, state) =>
            assertBalanceInvariantForSponsorship(blockDiff, executedSigner.toAddress, state.carryFee)

            val disableSender = disable.sender.toAddress
            disable.feeAssetId shouldBe Some(assetId)
            disable.fee shouldBe DisableFeeInAsset

            blockDiff.portfolios(disableSender).assets(assetId) shouldBe -DisableFeeInAsset
            blockDiff.portfolios(issuer).assets(assetId) shouldBe DisableFeeInAsset
            blockDiff.portfolios(issuer).balance shouldBe -DisableFee
        }
    }
  }
}

object SponsoredContractTransactionDiffTest {
  private val CreateFee           = fees.forTxType(CreateContractTransaction.typeId)
  private val CallFee             = fees.forTxType(CallContractTransaction.typeId)
  private val UpdateFee           = fees.forTxType(UpdateContractTransaction.typeId)
  private val DisableFee          = fees.forTxType(DisableContractTransaction.typeId)
  private val CreateFeeInAsset    = Sponsorship.fromWest(CreateFee)
  private val CallFeeInAsset      = Sponsorship.fromWest(CallFee)
  private val UpdateFeeInAsset    = Sponsorship.fromWest(UpdateFee)
  private val DisableFeeInAsset   = Sponsorship.fromWest(DisableFee)
  private val AssetTransferAmount = 100 * CreateFeeInAsset
}
