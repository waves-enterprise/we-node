package com.wavesenterprise.state.diffs.docker

import cats.implicits._
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state._
import com.wavesenterprise.state.diffs.TransferOpsSupport
import com.wavesenterprise.transaction.ValidationError.UnexpectedTransactionError
import com.wavesenterprise.transaction.docker.{CreateContractTransaction}
import com.wavesenterprise.transaction.{Signed, ValidationError}
import com.wavesenterprise.transaction.docker.{ConfidentialDataInCreateContractSupported, CreateContractTransaction}
import com.wavesenterprise.transaction.{PaymentsV1ToContract, Signed, ValidationError}

/**
  * Creates [[Diff]] for [[CreateContractTransaction]]
  */
case class CreateContractTransactionDiff(blockchain: Blockchain, blockOpt: Option[Signed], height: Int)
    extends ValidatorsValidator with BytecodeValidator
    with TransferOpsSupport {

  def apply(tx: CreateContractTransaction): Either[ValidationError, Diff] = {
    def checkConfidentialDataTxParams(tx: CreateContractTransaction): Either[ValidationError, Unit] = {
      tx match {
        case txWithPrivateDataSupport: ConfidentialDataInCreateContractSupported with PaymentsV1ToContract =>
          ConfidentialContractValidations.checkConfidentialCreateParamsSanity(txWithPrivateDataSupport, blockchain.lastBlockContractValidators)
        case _ =>
          Right(())
      }
    }

    blockOpt match {
      case Some(_) =>
        Left(UnexpectedTransactionError(tx))
      case None =>
        val contractInfo = ContractInfo(tx)

        checkTxVersionSupported(tx) >>
          checkConfidentialDataTxParams(tx) >>
          checkBytecode(contractInfo) >>
          checkValidators(contractInfo.validationPolicy) >> {
            val baseCreateContractDiff = Diff(
              height = height,
              tx = tx,
              contracts = Map(ContractId(contractInfo.contractId) -> contractInfo),
              portfolios = Diff.feeAssetIdPortfolio(tx, tx.sender.toAddress.toAssetHolder, blockchain)
            )
            tx match {
              case ctx: PaymentsV1ToContract if ctx.payments.nonEmpty =>
                for {
                  transfersDiff <- contractTransfersDiff(blockchain, tx, ctx.payments, height)
                } yield baseCreateContractDiff |+| transfersDiff
              case _ => baseCreateContractDiff.asRight
            }
          }
    }
  }

  private def checkTxVersionSupported(tx: CreateContractTransaction): Either[ValidationError, Unit] = {
    import com.wavesenterprise.features.FeatureProvider._

    if (tx.version == 1 && blockchain.isFeatureActivated(BlockchainFeature.ContractNativeTokenSupportAndPkiV1Support, height)) {
      Left {
        ValidationError.GenericError(
          s"CreateContractTransactionV1 is not allowed. Since feature '${BlockchainFeature.ContractNativeTokenSupportAndPkiV1Support.id}'" +
            " activation, REST-based smart-contracts are deprecated and cannot be created anymore.")
      }
    } else {
      Right(())
    }
  }
}
