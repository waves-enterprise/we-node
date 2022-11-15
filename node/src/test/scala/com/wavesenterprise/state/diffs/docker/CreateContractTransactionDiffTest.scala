package com.wavesenterprise.state.diffs.docker

import com.wavesenterprise.block.BlockFeeCalculator
import com.wavesenterprise.docker.validator.ValidationPolicy
import com.wavesenterprise.lagonaki.mocks.TestBlock.{create => block}
import com.wavesenterprise.settings.TestFunctionalitySettings.{EnabledForContractValidation, EnabledForNativeTokens}
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state.diffs.{ENOUGH_AMT, assertBalanceInvariant, assertDiffAndState, assertDiffEither, produce}
import com.wavesenterprise.state.{ContractId, LeaseBalance, Portfolio}
import com.wavesenterprise.transaction.AssetId
import com.wavesenterprise.transaction.docker.{ContractTransactionGen, CreateContractTransactionV5}
import com.wavesenterprise.transaction.validation.TransferValidation.MaxTransferCount
import com.wavesenterprise.{NoShrink, TransactionGen}
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CreateContractTransactionDiffTest
    extends AnyPropSpec
    with ScalaCheckPropertyChecks
    with Matchers
    with ContractTransactionGen
    with ExecutableTransactionGen
    with TransactionGen
    with NoShrink {

  property("Cannot use CreateContractTransaction with majority validation policy when empty contract validators") {
    forAll(preconditionsForCreateV4AndExecutedV2(validationPolicy = ValidationPolicy.Majority, proofsCount = 0)) {
      case (genesisBlock, executedSigner, executedCreate) =>
        assertDiffEither(Seq(genesisBlock), block(executedSigner, Seq(executedCreate)), fsForV4WithContractValidation) {
          _ should produce(s"Not enough network participants with 'contract_validator' role")
        }
    }
  }

  property("Cannot use CreateContractTransactionV5 with native token operations when ContractNativeTokenSupport feature not activated") {
    forAll(preconditionsForCreateV5AndCallV3ExecutedV3(minProofs = 1)) {
      case (genesisBlock, executedSigner, executedCreate, executedCall) =>
        assertDiffEither(Seq(genesisBlock), block(executedSigner, Seq(executedCreate, executedCall)), EnabledForContractValidation) {
          _ should produce(
            s"Blockchain feature 'Support of token operations for smart-contracts and PKI support v1' (id: '1120') has not been activated yet, but is required for ExecutedContractTransactionV3")
        }
    }
  }

  property("CreateContractTransactionV5 with transfers preserves balance invariant") {
    def checkInvariantWithParametrizedTransfers(withAsset: Boolean): Unit = {
      forAll(preconditionsForCreateV5WithTransfersOptWithAsset(withAsset, MaxTransferCount)) {
        case (prevBlocks, testerAcc, optIssue, executedCreate) =>
          assertDiffAndState(prevBlocks, block(testerAcc, Seq(executedCreate)), EnabledForNativeTokens) {
            case (totalDiff, newState) =>
              assertBalanceInvariant(totalDiff)
              val issueFee      = optIssue.map(_.fee).getOrElse(0L)
              val issueQuantity = optIssue.map(_.quantity).getOrElse(0L)
              val optAssetId    = optIssue.map(_.assetId())

              val validatorsFee = if (executedCreate.validationProofs.nonEmpty) {
                val executedFee     = executedCreate.tx.fee
                val validatorsCount = executedCreate.validationProofs.length
                val feePerValidator = BlockFeeCalculator.CurrentBlockValidatorsFeePart(executedFee) / validatorsCount

                feePerValidator * validatorsCount
              } else { 0L }

              val transfersOptAssetAmountMap =
                executedCreate.tx.asInstanceOf[CreateContractTransactionV5].payments.groupBy(_.assetId).mapValues(_.map(_.amount).sum)
              val transfersWestAmount  = transfersOptAssetAmountMap.getOrElse(None, 0L)
              val transfersAssetAmount = transfersOptAssetAmountMap.getOrElse(optAssetId, 0L)

              val testerAccWestSentAmount  = ENOUGH_AMT - (issueFee + transfersWestAmount + validatorsFee)
              val testerAccAssetSentAmount = optAssetId.fold(Map.empty[AssetId, Long])(aid => Map(aid -> (issueQuantity - transfersAssetAmount)))

              val testerPortfolioExpect = Portfolio(testerAccWestSentAmount, LeaseBalance.empty, testerAccAssetSentAmount)

              newState.assetHolderPortfolio(testerAcc.toAddress.toAssetHolder) shouldBe testerPortfolioExpect
              newState.assetHolderPortfolio(ContractId(executedCreate.tx.contractId).toAssetHolder) shouldBe Portfolio(
                transfersWestAmount,
                LeaseBalance.empty,
                transfersOptAssetAmountMap.filterKeys(_.isDefined).foldLeft(Map.empty[AssetId, Long]) {
                  case (res, (optAId, amount)) => res + (optAId.get -> amount)
                }
              )

          }
      }
    }

    checkInvariantWithParametrizedTransfers(withAsset = false)
    checkInvariantWithParametrizedTransfers(withAsset = true)
  }
}
