package com.wavesenterprise.state.diffs.docker

import com.wavesenterprise.NoShrink
import com.wavesenterprise.account.PrivateKeyAccount
import com.wavesenterprise.block.Block
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.lagonaki.mocks.TestBlock
import com.wavesenterprise.settings.{FunctionalitySettings, TestFunctionalitySettings}
import com.wavesenterprise.state.diffs.{ENOUGH_AMT, assertDiffEi, produce}
import com.wavesenterprise.transaction.docker.{ContractTransactionGen, ExecutedContractTransactionV2}
import com.wavesenterprise.transaction.{GenesisTransaction, Transaction}
import com.wavesenterprise.utils.EitherUtils.EitherExt
import org.scalacheck.Gen
import org.scalatest.{Matchers, PropSpec}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._

class CreateContractTransactionDiffTest extends PropSpec with ScalaCheckPropertyChecks with Matchers with ContractTransactionGen with NoShrink {

  property("Cannot use CreateContractTransaction with majority validation policy when empty contract validators") {
    forAll(preconditions(validationPolicy = ValidationPolicy.Majority, proofsCount = 0)) {
      case (genesisBlock, executedSigner, executedCreate) =>
        assertDiffEi(Seq(genesisBlock), TestBlock.create(executedSigner, Seq(executedCreate)), functionalitySettings) {
          _ should produce(s"Not enough network participants with 'contract_validator' role")
        }
    }
  }

  def preconditions(
      validationPolicy: ValidationPolicy,
      proofsCount: Int,
      additionalGenesisTxs: Seq[Transaction] = Seq.empty,
  ): Gen[(Block, PrivateKeyAccount, ExecutedContractTransactionV2)] =
    for {
      genesisTime    <- ntpTimestampGen.map(_ - 1.minute.toMillis)
      executedSigner <- accountGen
      create <- createContractV4ParamGen(Gen.const(None),
                                         (Gen.const(None), createTxFeeGen),
                                         accountGen,
                                         Gen.const(validationPolicy),
                                         contractApiVersionGen)
      executedCreate <- executedContractV2ParamGen(executedSigner, create, identity, identity, proofsCount)
      genesisForCreateAccount = GenesisTransaction.create(create.sender.toAddress, ENOUGH_AMT, genesisTime).explicitGet()
      genesisBlock            = TestBlock.create(Seq(genesisForCreateAccount) ++ additionalGenesisTxs)
    } yield (genesisBlock, executedSigner, executedCreate)

  val functionalitySettings: FunctionalitySettings = TestFunctionalitySettings.Enabled.copy(
    preActivatedFeatures = TestFunctionalitySettings.Enabled.preActivatedFeatures +
      (BlockchainFeature.ContractValidationsSupport.id -> 0)
  )
}
