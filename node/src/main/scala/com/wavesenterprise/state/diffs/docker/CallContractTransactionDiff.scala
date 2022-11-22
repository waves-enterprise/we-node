package com.wavesenterprise.state.diffs.docker

import cats.implicits._
import com.wavesenterprise.docker.ContractInfo
import com.wavesenterprise.features.BlockchainFeature
import com.wavesenterprise.state.AssetHolder._
import com.wavesenterprise.state.diffs.TransferOpsSupport
import com.wavesenterprise.state.{Blockchain, ContractId, Diff}
import com.wavesenterprise.transaction.ValidationError.{ContractIsDisabled, ContractNotFound, ContractVersionMatchError, UnexpectedTransactionError}
import com.wavesenterprise.transaction.docker.{CallContractTransaction, CallContractTransactionV1, CallContractTransactionV5, ExecutableTransaction}
import com.wavesenterprise.transaction.{Signed, ValidationError}

/**
  * Creates [[Diff]] for [[CallContractTransaction]]
  */
case class CallContractTransactionDiff(blockchain: Blockchain, blockOpt: Option[Signed], height: Int)
    extends ValidatorsValidator
    with TransferOpsSupport {

  def apply(tx: CallContractTransaction): Either[ValidationError, Diff] = {
    def checkContractIsNotLegacy(): Either[ValidationError, Unit] = {
      import com.wavesenterprise.features.FeatureProvider._

      if (blockchain.isFeatureActivated(BlockchainFeature.ContractNativeTokenSupportAndPkiV1Support, height)) {
        val validatedCreateTx: Either[ValidationError, ExecutableTransaction] = blockchain
          .executedTxFor(tx.contractId)
          .map(_.tx)
          .toRight(ValidationError.GenericError(s"Create contract tx '${tx.contractId}' not found the call tx '${tx.id()}'"))

        validatedCreateTx.flatMap { createTx =>
          if (createTx.version == 1) {
            Left(
              ValidationError.GenericError(
                "Not allowed to call contract which was created with " +
                  "CreateContractTransactionV1 since node version 1.12 and activated NativeTokens feature " +
                  "REST-based Smart-Contracts are deprecated and cannot be created anymore"))
          } else {
            Right(())
          }
        }
      } else {
        Right(())
      }
    }

    (blockOpt match {
      case Some(_) => Left(UnexpectedTransactionError(tx))
      case None =>
        lazy val baseCallContractDiff = for {
          contractInfo <- blockchain.contract(ContractId(tx.contractId)).toRight(ContractNotFound(tx.contractId))
          _            <- checkContractIsNotLegacy()
          _            <- checkContractVersion(tx, contractInfo)
          _            <- checkValidators(contractInfo.validationPolicy)
          _            <- Either.cond(contractInfo.active, (), ContractIsDisabled(tx.contractId))
        } yield Diff(height = height, tx = tx, portfolios = Diff.feeAssetIdPortfolio(tx, tx.sender.toAddress.toAssetHolder, blockchain))

        tx match {
          case ccTxV5: CallContractTransactionV5 if ccTxV5.payments.nonEmpty =>
            for {
              baseDiff      <- baseCallContractDiff
              transfersDiff <- contractTransfersDiff(blockchain, tx, ccTxV5.payments, height)
            } yield baseDiff |+| transfersDiff
          case _ => baseCallContractDiff
        }
    })
  }

  private def checkContractVersion(tx: CallContractTransaction, ci: ContractInfo): Either[ValidationError, Unit] = {
    tx match {
      case _: CallContractTransactionV1 =>
        Either.cond(ci.version == ContractInfo.FirstVersion, (), ContractVersionMatchError(ci, ContractInfo.FirstVersion))
      case ctxV2 =>
        Either.cond(ci.version == ctxV2.contractVersion, (), ContractVersionMatchError(ci, ctxV2.contractVersion))
    }
  }

}
