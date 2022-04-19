package com.wavesenterprise.state.diffs.docker

import cats.kernel.Monoid
import com.wavesenterprise.NoShrink
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.block.Block
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.{FunctionalitySettings, TestFunctionalitySettings}
import com.wavesenterprise.state.diffs._
import com.wavesenterprise.state.{DataEntry, Portfolio}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import com.wavesenterprise.transaction.GenesisTransaction
import com.wavesenterprise.transaction.docker._
import monix.eval.Coeval
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._

/**
  * Test for [[UpdateContractTransactionDiff]]
  */
class UpdateContractTransactionDiffTest extends PropSpec with ScalaCheckPropertyChecks with Matchers with ContractTransactionGen with NoShrink {

  private val fs = TestFunctionalitySettings.Enabled

  implicit class ListExt(list: List[DataEntry[_]]) {
    def asMap: Map[String, DataEntry[_]] = list.map(e => e.key -> e).toMap
  }

  val preconditions: Gen[(Seq[Block], PrivateKeyAccount, ExecutedContractTransactionV1)] = for {
    genesisTime    <- ntpTimestampGen.map(_ - 1.minute.toMillis)
    createSigner   <- accountGen
    create         <- createContractV2ParamGen(createSigner)
    executedCreate <- executedContractV1ParamGen(createSigner, create)
    update         <- Gen.oneOf(updateContractV1ParamGen(createSigner, create), updateContractV2ParamGenWithoutSponsoring(createSigner, create))
    executedUpdate <- executedForUpdateGen(createSigner, update)
    genesisForCreateAccount = GenesisTransaction.create(createSigner.toAddress, ENOUGH_AMT, genesisTime).explicitGet()
    genesisBlock            = TestBlock.create(createSigner, Seq(genesisForCreateAccount))
    executedTxBlock         = TestBlock.create(createSigner, Seq(executedCreate))
  } yield (Seq(genesisBlock, executedTxBlock), createSigner, executedUpdate)

  val withCallTxV1: Gen[(Seq[Block], CallContractTransactionV1, Block)] = for {
    (blocks, account, update) <- preconditions
    updateBlock = TestBlock.create(account, Seq(update))
    callTxV1     <- callContractV1ParamGen(account, update.tx.contractId)
    executedCall <- executedContractV1ParamGen(account, callTxV1)
    executedTxBlock = TestBlock.create(account, Seq(executedCall))
  } yield (blocks :+ updateBlock, callTxV1, executedTxBlock)

  val withCallTxV2: Gen[(Seq[Block], CallContractTransactionV2, Block)] = for {
    (blocks, account, update) <- preconditions
    updateBlock = TestBlock.create(account, Seq(update))
    callTxV2     <- callContractV2ParamGen(account, update.tx.contractId, 2)
    executedCall <- executedContractV1ParamGen(account, callTxV2)
    executedTxBlock = TestBlock.create(account, Seq(executedCall))
  } yield (blocks :+ updateBlock, callTxV2, executedTxBlock)

  val withCallTxV3: Gen[(Seq[Block], CallContractTransactionV3, Block)] = for {
    (blocks, account, update) <- preconditions
    updateBlock = TestBlock.create(account, Seq(update))
    callTxV3     <- callContractV3ParamGenWithoutSponsoring(account, update.tx.contractId, 2)
    executedCall <- executedContractV1ParamGen(account, callTxV3)
    executedTxBlock = TestBlock.create(account, Seq(executedCall))
  } yield (blocks :+ updateBlock, callTxV3, executedTxBlock)

  property("check update docker contract transaction diff and state") {
    forAll(preconditions) {
      case (blocks, minerAccount, executedUpdate) =>
        assertDiffAndState(blocks, TestBlock.create(minerAccount, Seq(executedUpdate)), fs) {
          case (blockDiff, state) =>
            val totalPortfolioDiff: Portfolio = Monoid.combineAll(blockDiff.portfolios.values)
            totalPortfolioDiff.balance shouldBe 0
            totalPortfolioDiff.effectiveBalance shouldBe 0
            totalPortfolioDiff.assets shouldBe Map.empty

            val updateTx = executedUpdate.tx.asInstanceOf[UpdateContractTransaction]

            // there is an Update tx and it's corresponding Executed tx
            blockDiff.transactions.size shouldBe 2
            blockDiff.transactionsMap.get(updateTx.id()) shouldNot be(None)

            state.transactionInfo(executedUpdate.id()).map(_._2) shouldBe Some(executedUpdate)
            state.executedTxFor(updateTx.id()) shouldBe Some(executedUpdate)

            state.contract(updateTx.contractId) shouldBe Some(
              ContractInfo(Coeval.pure(updateTx.sender), updateTx.contractId, updateTx.image, updateTx.imageHash, 2, active = true))
        }
    }
  }

  property("call transaction V1 should not proceed after update") {
    forAll(withCallTxV1) {
      case (blocks, callTxV1, executedCallTxV1Block) =>
        assertDiffEi(blocks, executedCallTxV1Block, fs) { blockDiffEi =>
          blockDiffEi should produce(s"Called version '1' of contract with id '${callTxV1.contractId}' doesn't match actual contract version '2'")
        }
    }
  }

  property("call transaction V2 should proceed after update") {
    forAll(withCallTxV2) {
      case (blocks, callTxV2, executedCallTxV2Block) =>
        assertDiffAndState(blocks, executedCallTxV2Block, fs) {
          case (blockDiff, state) =>
            blockDiff.transactions.size shouldBe 2 // executedTx + callTx
            blockDiff.transactionsMap.get(callTxV2.id()) shouldNot be(None)

            state.transactionInfo(callTxV2.id()).map(_._2) shouldBe Some(callTxV2)
        }
    }
  }

  property("call transaction V3 should proceed after update") {
    forAll(withCallTxV3) {
      case (blocks, callTxV3, executedCallTxV3Block) =>
        assertDiffAndState(blocks, executedCallTxV3Block, fs) {
          case (blockDiff, state) =>
            blockDiff.transactions.size shouldBe 2 // executedTx + callTx
            blockDiff.transactionsMap.get(callTxV3.id()) shouldNot be(None)

            state.transactionInfo(callTxV3.id()).map(_._2) shouldBe Some(callTxV3)
        }
    }
  }

  property("Cannot use UpdateContractTransaction with majority validation policy when empty contract validators") {
    val functionalitySettings: FunctionalitySettings = fs.copy(
      preActivatedFeatures = fs.preActivatedFeatures +
        (BlockchainFeature.ContractValidationsSupport.id -> 0)
    )

    forAll(preconditionsV2(validationPolicy = ValidationPolicy.Majority, proofsCount = 0)) {
      case (genesisBlock, executedSigner, executedCreate, executedUpdate) =>
        assertDiffEi(Seq(genesisBlock), TestBlock.create(executedSigner, Seq(executedCreate, executedUpdate)), functionalitySettings) {
          _ should produce(s"Not enough network participants with 'contract_validator' role")
        }
    }
  }

  def preconditionsV2(
      validationPolicy: ValidationPolicy,
      proofsCount: Int,
  ): Gen[(Block, PrivateKeyAccount, ExecutedContractTransactionV2, ExecutedContractTransactionV2)] =
    for {
      genesisTime    <- ntpTimestampGen.map(_ - 1.minute.toMillis)
      executedSigner <- accountGen
      create <- createContractV4ParamGen(Gen.const(None),
                                         (Gen.const(None), createTxFeeGen),
                                         executedSigner,
                                         Gen.const(ValidationPolicy.Any),
                                         contractApiVersionGen)
      executedCreate <- executedContractV2ParamGen(executedSigner, create, identity, identity, proofsCount)
      update <- updateContractV4ParamGen(
        atomicBadgeGen = Gen.const(None),
        feeAssetIdGen = (Gen.const(None), updateTxFeeGen),
        signerGen = executedSigner,
        validationPolicyGen = Gen.const(validationPolicy),
        contractApiVersionGen = contractApiVersionGen,
        contractIdGen = Gen.const(create.contractId),
      )
      executedUpdate <- executedContractV2ParamGen(executedSigner, update, identity, identity, proofsCount)
      genesisForCreateAccount = GenesisTransaction.create(create.sender.toAddress, ENOUGH_AMT, genesisTime).explicitGet()
      genesisBlock            = TestBlock.create(Seq(genesisForCreateAccount))
    } yield (genesisBlock, executedSigner, executedCreate, executedUpdate)
}
